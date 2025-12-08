import requests
import json
import paho.mqtt.client as mqtt
import time
import threading
import random
from datetime import datetime

# Thông tin API
SERVER_HOST = "https://lumentree.net"
DEVICE_ID = "H240805202"

# Thông tin MQTT
MQTT_BROKER = "192.168.1.32"
MQTT_PORT = 1883
MQTT_USER = "MQTT"
MQTT_PASSWORD = "123456"
CLIENT_ID = "LumenTree"

# Biến toàn cục
last_timestamp = ""
last_api_fetch_time = 0
API_FETCH_INTERVAL = 180  # 180 giây cho energy totals
last_session_refresh_time = 0
SESSION_REFRESH_INTERVAL = 3600  # 60 phút
api_session = requests.Session()

# Realtime polling
REALTIME_POLL_INTERVAL = 2  # Giây, match JS loadRealtime()
last_realtime_fetch_time = 0

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

# Hàm gửi thông tin khám phá cảm biến (giữ nguyên)
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

# Gửi tất cả thông tin khám phá cảm biến (giữ nguyên)
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

# Hàm gửi dữ liệu cảm biến (giữ nguyên)
def send_data_sensor(sensor_measurement, sensor_name, value):
    state_topic = f"{CLIENT_ID}/{sensor_measurement}/{sensor_name}"
    mqtt_client.publish(state_topic, str(value), retain=True)

# Hàm làm mới session (giữ nguyên)
def refresh_api_session():
    global api_session, last_session_refresh_time
    api_session.close()
    api_session = requests.Session()
    last_session_refresh_time = time.time()
    print("Đã làm mới phiên API")

# Hàm lấy dữ liệu energy totals từ API ngày (giữ nguyên, đã hoạt động)
def fetch_api_data():
    global last_api_fetch_time, last_session_refresh_time, api_session
    if time.time() - last_api_fetch_time < API_FETCH_INTERVAL:
        return

    if time.time() - last_session_refresh_time > SESSION_REFRESH_INTERVAL:
        refresh_api_session()
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    api_url = f"{SERVER_HOST}/api/day/{DEVICE_ID}/{current_date}"
    print(f"Gửi yêu cầu tới: {api_url}")

    try:
        response = api_session.get(api_url, headers={
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://lumentree.net",
            "Accept": "application/json"
        }, timeout=10)
        response.raise_for_status()
        data = response.json()

        pv_total = (data.get("pv_raw", {}).get("pv", {}).get("tableValue", 0) / 10.0)
        bat_charge = (data.get("bat_raw", {}).get("bats", [{}])[0].get("tableValue", 0) / 10.0)
        bat_discharge = (data.get("bat_raw", {}).get("bats", [{}])[1].get("tableValue", 0) / 10.0)
        load_total = (data.get("other_raw", {}).get("homeload", {}).get("tableValue", 0) / 10.0)
        grid_total = (data.get("other_raw", {}).get("grid", {}).get("tableValue", 0) / 10.0)
        essential_total = (data.get("other_raw", {}).get("essentialLoad", {}).get("tableValue", 0) / 10.0)

        send_data_sensor("energy", "pvTotal", pv_total)
        send_data_sensor("energy", "batCharge", bat_charge)
        send_data_sensor("energy", "batDischarge", bat_discharge)
        send_data_sensor("energy", "loadTotal", load_total)
        send_data_sensor("energy", "gridTotal", grid_total)
        send_data_sensor("energy", "essentialTotal", essential_total)

        print("=== Dữ liệu tổng hợp từ API ===")
        print(f"PV Total: {pv_total} kWh")
        print(f"Battery Charge: {bat_charge} kWh")
        print(f"Battery Discharge: {bat_discharge} kWh")
        print(f"Load Total: {load_total} kWh")
        print(f"Grid Total: {grid_total} kWh")
        print(f"Essential Load: {essential_total} kWh")

        last_api_fetch_time = time.time()

    except requests.Timeout:
        print("Yêu cầu API bị timeout sau 10 giây")
        refresh_api_session()
    except requests.RequestException as e:
        print(f"Lỗi khi gọi API: {e}")
        refresh_api_session()
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Lỗi parse dữ liệu API: {e}")

# SỬA: Hàm mới - Fetch realtime data từ /api/realtime (thay thế WebSocket)
def fetch_realtime_data():
    global last_realtime_fetch_time, last_timestamp, api_session
    if time.time() - last_realtime_fetch_time < REALTIME_POLL_INTERVAL:
        return

    realtime_url = f"{SERVER_HOST}/api/realtime/{DEVICE_ID}"
    print(f"Gửi yêu cầu realtime tới: {realtime_url}")

    try:
        response = api_session.get(realtime_url, headers={
            "User-Agent": "Mozilla/5.0",
            "Origin": "https://lumentree.net",
            "Accept": "application/json"
        }, timeout=5)  # Timeout ngắn hơn cho realtime
        response.raise_for_status()
        data = response.json()

        if not data or not data.get("data"):
            print("Không có dữ liệu realtime")
            return

        device_data = data["data"]
        last_timestamp = device_data.get("timestamp", last_timestamp)  # Cập nhật timestamp nếu có

        # Parse fields từ JS structure
        pv1V = float(device_data.get("pv1Voltage", 0) or 0)
        pv2V = float(device_data.get("pv2Voltage", 0) or 0)
        pv1P = float(device_data.get("pv1Power", 0) or 0)
        pv2P = float(device_data.get("pv2Power", 0) or 0)
        homeLoad = float(device_data.get("homeLoad", 0) or 0)
        essentialLoad = float(device_data.get("acOutputPower", 0) or 0)
        grid = float(device_data.get("gridPowerFlow", 0) or 0)
        gridVoltage = float(device_data.get("acInputVoltage", 0) or 0)
        batterySoc = float(device_data.get("batterySoc", 0) or 0)
        batteryVoltage = float(device_data.get("batteryVoltage", 0) or 0)
        batteryCurrent = float(device_data.get("batteryCurrent", 0) or 0)
        batteryPower = float(device_data.get("batteryPower", 0) or 0)
        temperature = float(device_data.get("temperature", 0) or 0)

        # Tính totalPv (chỉ cộng nếu voltage hợp lệ, như JS)
        pv1Valid = 50 < pv1V < 600
        pv2Valid = 50 < pv2V < 600
        totalPv = 0
        if pv1Valid: totalPv += pv1P
        if pv2Valid: totalPv += pv2P

        # Tính currents và inverter (như JS và on_message cũ)
        pv_current = totalPv / pv1V if pv1V else 0.0
        grid_current = grid / gridVoltage if gridVoltage else 0.0
        inverter_power = max(homeLoad - grid, 0)
        inverter_current = inverter_power / gridVoltage if gridVoltage else 0.0
        grid_frequency = device_data.get("acInputFrequency", 50 + (random.randint(-5, 5) / 10.0))  # Giả sử nếu không có

        # Sensors realtime (gửi nếu value hợp lệ)
        sensors = [
            ("temperature", "deviceTempValue", temperature),
            ("power", "essentialValue", essentialLoad),
            ("power", "gridValue", grid),
            ("power", "loadValue", homeLoad),
            ("power", "pv1Power", pv1P),
            ("power", "pv2Power", pv2P),
            ("power", "pvTotalPower", totalPv),
            ("power", "batteryValue", batteryPower),
            ("power", "InverterPower", inverter_power),
            ("battery", "batteryPercent", batterySoc),
            ("voltage", "batteryVoltage", batteryVoltage),
            ("voltage", "gridVoltageValue", gridVoltage),
            ("voltage", "pvVoltage", pv1V),  # Dùng pv1V làm đại diện
            ("current", "pvCurrent", pv_current),
            ("current", "gridCurrent", grid_current),
            ("current", "batteryCurrent", batteryCurrent),
            ("current", "InverterCurrent", inverter_current),
            ("frequency", "Gird_Frequency", grid_frequency),
        ]

        for measurement, name, value in sensors:
            if isinstance(value, (int, float)):
                if measurement == "battery" and not (0 <= value <= 100):  # batteryPercent
                    continue
                if value < 0 and measurement not in ["power", "current"]:
                    continue
                send_data_sensor(measurement, name, value)

        print("=== Realtime Data ===")
        print(f"PV Total Power: {totalPv} W | Load: {homeLoad} W | Grid: {grid} W | Battery SOC: {batterySoc} %")
        print(f"PV Current: {pv_current:.1f} A | Grid Current: {grid_current:.1f} A | Inverter Power: {inverter_power} W")
        print(f"Timestamp: {last_timestamp}")

        last_realtime_fetch_time = time.time()

    except requests.Timeout:
        print("Yêu cầu realtime timeout sau 5 giây")
    except requests.RequestException as e:
        print(f"Lỗi khi gọi realtime API: {e}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Lỗi parse realtime data: {e}")

# SỬA: Loop cho realtime polling (thay thế run_websocket)
def realtime_poll_loop():
    while True:
        fetch_realtime_data()
        time.sleep(REALTIME_POLL_INTERVAL)  # Poll mỗi 2 giây

# Hàm chạy API fetch định kỳ (giữ nguyên)
def api_fetch_loop():
    while True:
        fetch_api_data()
        time.sleep(10)

# Main (SỬA: Thay WebSocket thread bằng realtime_poll_loop)
if __name__ == "__main__":
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()

    # Khởi động realtime polling trong thread riêng
    threading.Thread(target=realtime_poll_loop, daemon=True).start()
    # Khởi động API fetch loop trong thread riêng
    threading.Thread(target=api_fetch_loop, daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mqtt_client.loop_stop()
        print("Đã dừng chương trình")