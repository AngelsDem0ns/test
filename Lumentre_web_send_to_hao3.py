import requests
import websocket
import json
import paho.mqtt.client as mqtt
import time
import threading
import random
from datetime import datetime

# Thông tin WebSocket
WEBSOCKET_SERVER = "wss://lumentree.site/deviceHub"
SERVER_HOST = "https://lumentree.site"
DEVICE_ID = "H240805202"

# Thông tin MQTT
MQTT_BROKER = "192.168.1.100"
MQTT_PORT = 1883
MQTT_USER = "MQTT"
MQTT_PASSWORD = "88888888"
CLIENT_ID = "LumenTree"

# Biến toàn cục
last_timestamp = ""
last_api_fetch_time = 0
API_FETCH_INTERVAL = 180  # 180 giây
SESSION_REFRESH_INTERVAL = 3600  # 60 phút (3600 giây)
last_session_refresh_time = 0  # Thời gian làm mới phiên cuối cùng
api_session = requests.Session()  # Phiên requests ban đầu

# MQTT client
mqtt_client = mqtt.Client(client_id=CLIENT_ID)

# Hàm kết nối MQTT
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print("Đã kết nối tới MQTT Broker")
        publish_discovery_sensors()
    else:
        print(f"Kết nối thất bại, mã lỗi: {rc}")

def on_message(client, userdata, msg):
    print(f"Nhận được từ topic [{msg.topic}]: {msg.payload.decode()}")

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

# Hàm gửi thông tin khám phá cảm biến
def publish_discovery_sensor(sensor_measurement, sensor_name, friendly_name=""):
    sensor_units = {
        "temperature": "°C",
        "power": "W",
        "energy": "kWh",
        "battery": "%",
        "current": "A",
        "voltage": "V",
        "frequency": "Hz"
    }.get(sensor_measurement, "")

    discovery_topic = f"homeassistant/sensor/{CLIENT_ID}_{sensor_measurement}_{sensor_name}/config"
    state_topic = f"{CLIENT_ID}/{sensor_measurement}/{sensor_name}"
    if not friendly_name:
        friendly_name = f"{CLIENT_ID} {sensor_name} {sensor_measurement}"

    payload = {
        "name": friendly_name,
        "state_topic": state_topic,
        "unit_of_measurement": sensor_units,
        "device_class": sensor_measurement,
        "unique_id": f"{CLIENT_ID}_{sensor_measurement}_{sensor_name}",
        "device": {
            "identifiers": [CLIENT_ID],
            "name": CLIENT_ID,
            "manufacturer": "LumenTree",
            "model": "Energy Monitor"
        }
    }

    mqtt_client.publish(discovery_topic, json.dumps(payload), retain=True)
    print(f"Published discovery for sensor {sensor_name}")

# Gửi tất cả thông tin khám phá cảm biến
def publish_discovery_sensors():
    sensors = [
        ("energy", "pvTotal", "LumenTree PV Total Energy"),
        ("energy", "batCharge", "LumenTree Battery Charge Energy"),
        ("energy", "batDischarge", "LumenTree Battery Discharge Energy"),
        ("energy", "loadTotal", "LumenTree Load Total Energy"),
        ("energy", "gridTotal", "LumenTree Grid Total Energy"),
        ("energy", "essentialTotal", "LumenTree Essential Load Energy"),
        ("temperature", "deviceTempValue", "LumenTree Device Temperature"),
        ("power", "essentialValue", "LumenTree Essential Load"),
        ("power", "gridValue", "LumenTree Grid Value"),
        ("power", "loadValue", "LumenTree Load Value"),
        ("power", "pv1Power", "LumenTree PV1 Power"),
        ("power", "pv2Power", "LumenTree PV2 Power"),
        ("power", "pvTotalPower", "LumenTree Total PV Power"),
        ("power", "batteryValue", "LumenTree Battery Power"),
        ("power", "InverterPower", "LumenTree Inverter Power"),
        ("battery", "batteryPercent", "LumenTree Battery Percentage"),
        ("voltage", "batteryVoltage", "LumenTree Battery Voltage"),
        ("voltage", "gridVoltageValue", "LumenTree Grid Voltage"),
        ("voltage", "pvVoltage", "LumenTree PV Voltage"),
        ("current", "pvCurrent", "LumenTree PV Current"),
        ("current", "gridCurrent", "LumenTree Grid Current"),
        ("current", "batteryCurrent", "LumenTree Battery Current"),
        ("current", "InverterCurrent", "LumenTree Inverter Current"),
        ("frequency", "Gird_Frequency", "LumenTree Gird Frequency"),
    ]
    for sensor in sensors:
        publish_discovery_sensor(*sensor)

# Hàm gửi dữ liệu cảm biến
def send_data_sensor(sensor_measurement, sensor_name, value):
    state_topic = f"{CLIENT_ID}/{sensor_measurement}/{sensor_name}"
    mqtt_client.publish(state_topic, str(value), retain=True)

# Hàm làm mới session
def refresh_api_session():
    global api_session, last_session_refresh_time
    # Đóng phiên cũ
    api_session.close()
    # Tạo phiên mới
    api_session = requests.Session()
    last_session_refresh_time = time.time()
    print("Đã làm mới phiên API")

# Hàm lấy dữ liệu từ API
def fetch_api_data():
    global last_api_fetch_time, last_session_refresh_time, api_session
    if time.time() - last_api_fetch_time < API_FETCH_INTERVAL:
        return

    # Kiểm tra nếu cần làm mới phiên (sau 60 phút)
    if time.time() - last_session_refresh_time > SESSION_REFRESH_INTERVAL:
        refresh_api_session()
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    #current_date = datetime.now().strftime("%Y-%m-%d") if last_timestamp else ""
    # Lấy ngày trực tiếp từ last_timestamp
    #current_date = datetime.fromisoformat(last_timestamp).strftime("%Y-%m-%d") if last_timestamp else ""
    #if not current_date:
    #    print("Chưa nhận được timestamp, bỏ qua API fetch")
    #    return

    api_url = f"{SERVER_HOST}/device/{DEVICE_ID}?date={current_date}"
    print(f"Gửi yêu cầu tới: {api_url}")

    try:
        response = api_session.get(api_url, headers={
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://lumentree.site",
            "Accept": "application/json"
        }, timeout=10)  # Thêm timeout 10 giây
        response.raise_for_status()
        data = response.json()

        pv_total = data["pv"]["tableValue"] / 10.0
        bat_charge = data["bat"]["bats"][0]["tableValue"] / 10.0
        bat_discharge = data["bat"]["bats"][1]["tableValue"] / 10.0
        load_total = data["load"]["tableValue"] / 10.0
        grid_total = data["grid"]["tableValue"] / 10.0
        essential_total = data["essentialLoad"]["tableValue"] / 10.0

        send_data_sensor("energy", "pvTotal", pv_total)
        send_data_sensor("energy", "batCharge", bat_charge)
        send_data_sensor("energy", "batDischarge", bat_discharge)
        send_data_sensor("energy", "loadTotal", load_total)
        send_data_sensor("energy", "gridTotal", grid_total)
        send_data_sensor("energy", "essentialTotal", essential_total)

        print("=== Dữ liệu tổng hợp từ API ===")
        print(f"PV Total: {pv_total} kW")
        print(f"Battery Charge: {bat_charge} kW")
        print(f"Battery Discharge: {bat_discharge} kW")
        print(f"Load Total: {load_total} kW")
        print(f"Grid Total: {grid_total} kW")
        print(f"Essential Load: {essential_total} kW")

        last_api_fetch_time = time.time()

    except requests.Timeout:
        print("Yêu cầu API bị timeout sau 10 giây")
        refresh_api_session()  # Làm mới phiên ngay lập tức
    except requests.RequestException as e:
        print(f"Lỗi khi gọi API: {e}")
        refresh_api_session()  # Làm mới phiên nếu có lỗi khác

# Hàm xử lý WebSocket
def on_message(ws, message):
    global last_timestamp
    message = message.rstrip("\x1E")
    print(f"Tin nhắn nhận được: {message}")

    try:
        data = json.loads(message)
        if data.get("type") == 1 and data.get("target") == "ReceiveRealTimeData":
            args = data.get("arguments", [])
            if args:
                device_data = args[0]
                last_timestamp = device_data["timestamp"]

                pv_current = device_data["pvTotalPower"] / device_data["pv1Voltage"] if device_data["pv1Voltage"] else 0.0
                grid_current = device_data["gridValue"] / device_data["gridVoltageValue"] if device_data["gridVoltageValue"] else 0.0
                inverter_power = max(device_data["loadValue"] - device_data["gridValue"], 0)
                inverter_current = inverter_power / device_data["gridVoltageValue"] if device_data["gridVoltageValue"] else 0.0
                grid_frequency = 50 + (random.randint(-5, 5) / 10.0) if device_data["gridVoltageValue"] else 0.0

                sensors = [
                    ("temperature", "deviceTempValue", device_data["deviceTempValue"]),
                    ("power", "essentialValue", device_data["essentialValue"]),
                    ("power", "gridValue", device_data["gridValue"]),
                    ("power", "loadValue", device_data["loadValue"]),
                    ("power", "pv1Power", device_data["pv1Power"]),
                    ("power", "pv2Power", device_data.get("pv2Power", 0)),
                    ("power", "pvTotalPower", device_data["pvTotalPower"]),
                    ("power", "batteryValue", device_data["batteryValue"]),
                    ("power", "InverterPower", inverter_power),
                    ("battery", "batteryPercent", device_data["batteryPercent"]),
                    ("voltage", "batteryVoltage", device_data["batteryVoltage"]),
                    ("voltage", "gridVoltageValue", device_data["gridVoltageValue"]),
                    ("voltage", "pvVoltage", device_data["pv1Voltage"]),
                    ("current", "pvCurrent", pv_current),
                    ("current", "gridCurrent", grid_current),
                    ("current", "batteryCurrent", device_data["batteryCurrent"]),
                    ("current", "InverterCurrent", inverter_current),
                    ("frequency", "Gird_Frequency", grid_frequency),
                ]

                for measurement, name, value in sensors:
                    if isinstance(value, (int, float)):
                        if measurement == "batteryPercent" and not (0 <= value <= 100):
                            continue
                        if value < 0 and measurement not in ["power", "current"]:
                            continue
                        send_data_sensor(measurement, name, value)

                print("=== Real-time Data ===")
                for key, value in device_data.items():
                    print(f"{key}: {value}")
                print(f"PV Current: {pv_current} A")
                print(f"Grid Current: {grid_current} A")
                print(f"Inverter Power: {inverter_power} W")
                print(f"Inverter Current: {inverter_current} A")
                print(f"Grid Frequency: {grid_frequency} Hz")

    except json.JSONDecodeError as e:
        print(f"Lỗi phân tích JSON: {e}")

def on_error(ws, error):
    print(f"WebSocket lỗi: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket ngắt kết nối: {close_msg} (Mã: {close_status_code})")

def on_open(ws):
    print("WebSocket đã kết nối")
    ws.send('{"protocol":"json","version":1}\x1E')
    subscribe_msg = json.dumps({"type": 1, "target": "SubscribeToDevice", "arguments": [DEVICE_ID]}) + "\x1E"
    ws.send(subscribe_msg)

# Hàm chạy API fetch định kỳ
def api_fetch_loop():
    while True:
        fetch_api_data()
        time.sleep(10)

# Hàm chạy WebSocket với cơ chế kết nối lại
def run_websocket():
    while True:
        try:
            ws = websocket.WebSocketApp(
                WEBSOCKET_SERVER,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # Chạy WebSocket với cơ chế tự động kết nối lại
            ws.run_forever(
                ping_interval=30,  # Gửi ping mỗi 30 giây để giữ kết nối
                ping_timeout=10,   # Thời gian chờ phản hồi ping
                reconnect=5        # Thử kết nối lại sau 5 giây nếu mất kết nối
            )
        except Exception as e:
            print(f"Lỗi WebSocket tổng quát: {e}")
        print("Đang thử kết nối lại WebSocket sau 5 giây...")
        time.sleep(5)  # Chờ trước khi thử kết nối lại

# Main
if __name__ == "__main__":
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Khởi động WebSocket trong một luồng riêng
    threading.Thread(target=run_websocket, daemon=True).start()
    # Khởi động API fetch loop trong một luồng riêng
    threading.Thread(target=api_fetch_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mqtt_client.loop_stop()
        print("Đã dừng chương trình")