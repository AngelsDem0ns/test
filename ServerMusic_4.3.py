import asyncio
import aiohttp
from aiohttp import web
import yt_dlp
import json
import logging
import urllib.parse
import os
import hashlib
import aiofiles
import re
import subprocess
import psutil
import gc  # Garbage collect
from mutagen.mp3 import MP3  # pip install mutagen

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Secret key phải match ESP32
SECRET_KEY = "your-esp32-secret-key-2024"

# Headers YouTube
YOUTUBE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'audio/webm,audio/ogg,audio/mpeg,audio/*;q=0.9,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
    'Accept-Encoding': 'identity',
    'Referer': 'https://www.youtube.com/',
    'Origin': 'https://www.youtube.com',
}

# Thư mục cache
CACHE_DIR = 'music_cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# Semaphore for concurrent
background_semaphore = asyncio.Semaphore(3)

def log_memory(prefix=""):
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    logger.info(f"{prefix} Memory usage: {memory_mb:.2f} MB")

def generate_hash(query):
    normalized = query.lower().strip()
    return hashlib.sha256(normalized.encode()).hexdigest()[:32]

def parse_artist_from_title(title, query_artist=''):
    if query_artist and query_artist.strip():
        return query_artist.strip()
    
    match = re.match(r'^(.+?)\s*[-–]\s*(.+)$', title.strip())
    if match:
        artist = match.group(1).strip()
        artist = re.sub(r'^\[.*?\]\s*', '', artist)
        return artist
    
    if ' ' in query_artist:
        return query_artist.split(' ', 1)[0]
    
    return 'Unknown'

def verify_auth(request):
    mac = request.headers.get('X-MAC-Address', '')
    chip_id = request.headers.get('X-Chip-ID', '')
    timestamp_str = request.headers.get('X-Timestamp', '')
    dynamic_key = request.headers.get('X-Dynamic-Key', '')
    
    logger.info(f"Auth headers received: MAC={mac}, ChipID={chip_id}, TS={timestamp_str}, Key={dynamic_key}")
    
    if not all([mac, chip_id, timestamp_str, dynamic_key]):
        logger.warning(f"Missing auth headers")
        return False
    
    try:
        timestamp = int(timestamp_str)
        data = f"{mac}:{chip_id}:{timestamp}:{SECRET_KEY}"
        hash_obj = hashlib.sha256(data.encode())
        expected_key = hash_obj.hexdigest()[:32].upper()
        
        logger.info(f"Computed expected key: {expected_key}, received: {dynamic_key.upper()}")
        
        if dynamic_key.upper() != expected_key:
            logger.warning(f"Invalid dynamic key")
            return False
        
        logger.info("Auth successful")
        return True
    except ValueError as e:
        logger.error(f"Invalid timestamp: {e}")
        return False

async def auth_middleware(app, handler):
    async def middleware(request):
        if request.path.startswith('/music_cache/'):
            return await handler(request)
        
        if not verify_auth(request):
            return web.json_response({'error': 'Unauthorized'}, status=401)
        
        return await handler(request)
    return middleware

def cleanup_temp_files(cache_filename):
    temp_extensions = ['.part', '.ytdl', '.webm', '.m4a', '.tmp']
    for ext in temp_extensions:
        temp_path = os.path.join(CACHE_DIR, f"{cache_filename}{ext}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
            logger.info(f"Cleaned temp file: {temp_path}")

def kill_ffmpeg_processes():
    for proc in psutil.process_iter(['pid', 'name']):
        if 'ffmpeg' in proc.info['name'].lower():
            proc.kill()
            logger.info(f"Killed hanging FFmpeg PID {proc.info['pid']}")

def create_fallback_mp3(mp3_path, title, artist, duration=180):
    """Tạo fallback MP3 with FFmpeg: silence + loud beep, add ID3 tag."""
    try:
        # Create silence full
        silence_path = mp3_path + '.silence.tmp'
        cmd_silence = [
            'ffmpeg', '-y',
            '-f', 'lavfi', '-i', 'anullsrc=r=44100:cl=stereo',
            '-t', str(duration),
            '-ar', '22050', '-ac', '1',
            '-c:a', 'mp3', '-b:a', '96k',
            '-loglevel', 'quiet',
            silence_path
        ]
        subprocess.run(cmd_silence, capture_output=True, timeout=30)
        
        # Create loud beep (440Hz, 3s, volume -5dB)
        beep_path = mp3_path + '.beep.tmp'
        cmd_beep = [
            'ffmpeg', '-y',
            '-f', 'lavfi', '-i', 'sine=frequency=440:duration=3',
            '-af', 'volume=-5dB',  # Loud but not distort
            '-ar', '22050', '-ac', '1',
            '-c:a', 'mp3', '-b:a', '96k',
            '-loglevel', 'quiet',
            beep_path
        ]
        subprocess.run(cmd_beep, capture_output=True, timeout=10)
        
        # Concat beep + silence
        cmd_concat = [
            'ffmpeg', '-y',
            '-i', beep_path,
            '-i', silence_path,
            '-filter_complex', 'concat=n=2:v=0:a=1',
            '-c:a', 'mp3', '-b:a', '96k',
            '-loglevel', 'quiet',
            mp3_path
        ]
        result = subprocess.run(cmd_concat, capture_output=True, text=True, timeout=60)
        
        # Cleanup temp
        for tmp in [silence_path, beep_path]:
            if os.path.exists(tmp):
                os.remove(tmp)
        
        # Add ID3 tag
        if os.path.exists(mp3_path):
            audio = MP3(mp3_path)
            audio.tags.add(MP3.ADDR.TIT2, title)
            audio.tags.add(MP3.ADDR.TPE1, artist)
            audio.save()
        
        if result.returncode == 0:
            size = os.path.getsize(mp3_path) / 1024 / 1024
            logger.info(f"Created FFmpeg fallback beep + silence MP3 ({duration}s, size {size:.2f} MB): {mp3_path}")
            return True
        else:
            logger.error(f"FFmpeg fallback fail: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Fallback MP3 error: {e}")
        return False

def create_fallback_lyrics(title, artist, lrc_path):
    fallback = f"""[00:00.00]Lyrics for {title} by {artist}
[00:15.00]Verse 1: Bắt đầu bài hát {title}...
[01:00.00]Chorus: Điệp khúc vang vọng...
[02:00.00]Verse 2: Tiếp tục...
[03:00.00]Kết thúc bài hát."""
    with open(lrc_path, 'w', encoding='utf-8') as f:
        f.write(fallback)
    logger.info(f"Created fallback lyrics: {lrc_path}")

# Xoá cache cũ nếu vượt quá 500 MB
async def cleanup_old_cache(max_size_mb=500):
    total_size = 0
    files = []
    for name in os.listdir(CACHE_DIR):
        path = os.path.join(CACHE_DIR, name)
        if os.path.isfile(path):
            size = os.path.getsize(path)
            total_size += size
            files.append((path, size, os.path.getmtime(path)))
    total_size_mb = total_size / 1024 / 1024
    if total_size_mb > max_size_mb:
        files.sort(key=lambda x: x[2])  # sort by mtime
        while total_size_mb > max_size_mb * 0.8 and files:
            f, s, _ = files.pop(0)
            try:
                os.remove(f)
                total_size_mb -= s / 1024 / 1024
                logger.info(f"Deleted old cache: {f}")
            except:
                pass


async def background_download(full_query, cache_filename, title, artist, duration):
    async with background_semaphore:
        log_memory("Before background download")
        mp3_path = os.path.join(CACHE_DIR, f"{cache_filename}.mp3")
        lrc_path = os.path.join(CACHE_DIR, f"{cache_filename}.lrc")
        
        if os.path.exists(mp3_path) and os.path.getsize(mp3_path) > 100000:
            logger.info(f"Cache hit, skipping background for {title}")
            return
        
        logger.info(f"Starting background download for {full_query}")
        
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(CACHE_DIR, f"{cache_filename}.%(ext)s"),
            'http_headers': YOUTUBE_HEADERS,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'retries': 2,
            'fragment_retries': 2,
            'socket_timeout': 10,
            'geo_bypass': True,
            'no_cache_dir': True,
            'sleep_interval': 1,
            'max_sleep_interval': 3,
            'extractor_args': {
                'youtube': {'player_skip': 'js', 'skip': ['dash', 'hls']},
            },
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                #'preferredquality': '96',
                'preferredquality': '64',
            }],
            'postprocessor_args': {
                'FFmpegExtractAudio': ['-ar', '22050', '-ac', '1', '-vn'],
            },
        }
        old_size = os.path.getsize(mp3_path) if os.path.exists(mp3_path) else 0
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([f"ytsearch1:{full_query}"])
            # Rename
            for ext in ['mp3', 'webm', 'm4a']:
                temp_path = os.path.join(CACHE_DIR, f"{cache_filename}.{ext}")
                if os.path.exists(temp_path):
                    os.rename(temp_path, mp3_path)
                    break
            cleanup_temp_files(cache_filename)
            new_size = os.path.getsize(mp3_path)
            logger.info(f"Background real MP3 downloaded, overwrote ({old_size/1024/1024:.2f} -> {new_size/1024/1024:.2f} MB): {mp3_path}")
        except Exception as e:
            logger.error(f"Background download failed: {e}")
            cleanup_temp_files(cache_filename)
            kill_ffmpeg_processes()
        
        log_memory("After background download")
        gc.collect()
        await cleanup_old_cache()

    if not os.path.exists(lrc_path):
        create_fallback_lyrics(title, artist, lrc_path)

async def get_music_metadata(full_query, cache_filename, query_artist=''):
    mp3_path = os.path.join(CACHE_DIR, f"{cache_filename}.mp3")
    lrc_path = os.path.join(CACHE_DIR, f"{cache_filename}.lrc")
    
    from_cache = os.path.exists(mp3_path) and os.path.getsize(mp3_path) > 100000
    title = full_query
    artist = query_artist or 'Unknown'
    duration = 180
    cover_url = "http://y.gtimg.cn/music/photo_new/T002R300x300M000004AfbeH1xUvTe.jpg"
    
    if from_cache:
        size = os.path.getsize(mp3_path) / 1024 / 1024
        logger.info(f"Cache hit ({size:.2f} MB): {mp3_path}")
    else:
        logger.info(f"Cache miss, creating fallback full duration for {full_query}")
        
        # FFmpeg fallback
        #create_fallback_mp3(mp3_path, duration, title, artist)  # Add ID3
        create_fallback_mp3(mp3_path, title, artist, duration=3)
        
        # Fallback lyrics
        create_fallback_lyrics(title, artist, lrc_path)
        
        # Extract info NHANH
        ydl_opts = {
            'format': 'bestaudio/best',
            'http_headers': YOUTUBE_HEADERS,
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"ytsearch1:{full_query}", download=False)
                title = info.get('title', title)
                artist = parse_artist_from_title(title, query_artist) or info.get('uploader', artist)
                duration = info.get('duration', duration)
                if 'thumbnail' in info:
                    cover_url = info['thumbnail'].replace('default.jpg', 'maxresdefault.jpg')
        except Exception as e:
            logger.warning(f"Fast extract fail: {e}")
        
        # Background real download
        asyncio.create_task(background_download(full_query, cache_filename, title, artist, duration))
    
    logger.info(f"Metadata ready: {title} by {artist}, duration {duration}s, FFmpeg fallback ready")
    
    return {
        'artist': artist,
        'title': title,
        'audio_url': f"/music_cache/{cache_filename}.mp3",
        'cover_url': cover_url,
        'duration': duration,
        'from_cache': from_cache,
        'lyric_url': f"/music_cache/{cache_filename}.lrc"
    }

async def stream_pcm(request):
    try:
        song = urllib.parse.unquote(request.query.get('song', ''))
        artist = urllib.parse.unquote(request.query.get('artist', ''))
        
        if not song:
            return web.json_response({'error': 'Missing song param'}, status=400)
        
        full_query = f"{song}"
        if artist:
            full_query += f" {artist}"
        
        logger.info(f"Music request: song='{song}', artist='{artist}', query='{full_query}'")
        
        normalized_query = full_query.lower().strip()
        cache_filename = generate_hash(normalized_query)
        logger.info(f"Normalized for hash: '{normalized_query}', Cache filename: {cache_filename}")
        
        metadata = await get_music_metadata(full_query, cache_filename, artist)
        
        logger.info(f"Metadata returned for {metadata['title']} ({metadata['duration']}s)")
        return web.json_response({
            'success': True,
            **metadata
        })
    
    except Exception as e:
        logger.error(f"Error in stream_pcm: {str(e)}", exc_info=True)
        return web.json_response({'error': f'Server error: {str(e)}'}, status=500)
    finally:
        gc.collect()
        log_memory("After stream_pcm")

# /search (omit)
async def search_music(request):
    pass

async def main():
    app = web.Application(middlewares=[auth_middleware])
    
    app.router.add_post('/search', search_music)
    app.router.add_get('/search', search_music)
    app.router.add_get('/stream_pcm', stream_pcm)
    
    app.router.add_static('/music_cache/', path=CACHE_DIR, show_index=False)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '192.168.1.17', 5005)
    await site.start()
    
    logger.info("Server started on http://192.168.1.17:5005 (FFmpeg silence + loud beep, ID3 tag)")
    logger.info("Diy by me!")
    
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())