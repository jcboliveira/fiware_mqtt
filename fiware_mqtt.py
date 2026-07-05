#!/usr/bin/env python3

import json
import time
import os
import threading
import requests
import paho.mqtt.client as mqtt
from datetime import datetime, timezone, timedelta
import argparse

# ---------------------------------------------------------
# ARGUMENTOS CLI
# ---------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="FIWARE → MQTT Bridge (CLI)")

    parser.add_argument("--stations", type=str,
                        help="Lista de estações permitidas (nomes separados por vírgulas)")
    parser.add_argument("--exclude", type=str,
                        help="Lista de estações a excluir (nomes separados por vírgulas)")

    parser.add_argument("--mqtt-host", type=str, required=True,
                        help="Endereço IP do broker MQTT")
    parser.add_argument("--mqtt-user", type=str, required=True,
                        help="Username do broker MQTT")
    parser.add_argument("--mqtt-pass", type=str, required=True,
                        help="Password do broker MQTT")

    parser.add_argument("--list-stations", action="store_true",
                        help="Lista as estações disponíveis e termina")

    return parser.parse_args()

def station_allowed(local_name, args):
    name = local_name.lower()

    if args.stations:
        allowed = [s.strip().lower() for s in args.stations.split(",")]
        return name in allowed

    if args.exclude:
        blocked = [s.strip().lower() for s in args.exclude.split(",")]
        return name not in blocked

    return True


# ---------------------------------------------------------
# CONFIGURAÇÃO
# ---------------------------------------------------------
FIWARE_URL = "https://broker.fiware.urbanplatform.portodigital.pt/v2/entities"

MQTT_PORT = 1883

# ---------------------------------------------------------
# MQTT
# ---------------------------------------------------------
mqtt_connected = False
mqtt_lock = threading.Lock()

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    mqtt_connected = (rc == 0)
    if rc == 0:
        print("INFO: Ligado ao broker MQTT.")
    else:
        print(f"ERRO: Falha ao ligar ao broker (rc={rc}).")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    print("AVISO: Desligado do broker MQTT.")

def mqtt_reconnect_loop(client, args):
    global mqtt_connected
    delay = 1
    while True:
        if not mqtt_connected:
            with mqtt_lock:
                try:
                    print(f"AVISO: A tentar reconectar ao MQTT (delay={delay}s)…")
                    client.connect(args.mqtt_host, MQTT_PORT, 60)
                    delay = 1
                except Exception as e:
                    print(f"ERRO: Reconnect falhou: {e}")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)
        time.sleep(1)

def mqtt_init(args):
    client = mqtt.Client()
    client.username_pw_set(args.mqtt_user, args.mqtt_pass)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    while True:
        try:
            client.connect(args.mqtt_host, MQTT_PORT, 60)
            break
        except Exception as e:
            print(f"ERRO: Falha inicial ao ligar ao MQTT: {e}. Nova tentativa em 3s…")
            time.sleep(3)

    client.loop_start()
    threading.Thread(target=mqtt_reconnect_loop, args=(client, args), daemon=True).start()
    return client

# ---------------------------------------------------------
# Station name
# ---------------------------------------------------------
def get_station_name(entity):
    return (
        entity.get("name", {})
              .get("value", "Desconhecido")
              .strip()
    )

# ---------------------------------------------------------
# AQI
# ---------------------------------------------------------
def calc_aqi(value, breakpoints):
    for bp in breakpoints:
        c_low, c_high, aqi_low, aqi_high = bp
        if c_low <= value <= c_high:
            return round(((aqi_high - aqi_low) / (c_high - c_low)) * (value - c_low) + aqi_low)
    return None

def compute_aqi(entity):
    aqi_values = []

    if "pm25" in entity:
        aqi = calc_aqi(entity["pm25"]["value"], [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
        ])
        if aqi: aqi_values.append(aqi)

    if "pm10" in entity:
        aqi = calc_aqi(entity["pm10"]["value"], [
            (0, 54, 0, 50),
            (55, 154, 51, 100),
            (155, 254, 101, 150),
        ])
        if aqi: aqi_values.append(aqi)

    if "o3" in entity:
        aqi = calc_aqi(entity["o3"]["value"], [
            (0, 100, 0, 100),
            (101, 160, 101, 150),
        ])
        if aqi: aqi_values.append(aqi)

    if "no2" in entity:
        aqi = calc_aqi(entity["no2"]["value"], [
            (0, 100, 0, 100),
            (101, 200, 101, 150),
        ])
        if aqi: aqi_values.append(aqi)

    return max(aqi_values) if aqi_values else None

def compute_main_pollutant(entity):
    pollutants = {}

    if "pm25" in entity:
        pollutants["pm25"] = calc_aqi(entity["pm25"]["value"], [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
        ])

    if "pm10" in entity:
        pollutants["pm10"] = calc_aqi(entity["pm10"]["value"], [
            (0, 54, 0, 50),
            (55, 154, 51, 100),
            (155, 254, 101, 150),
        ])

    if "o3" in entity:
        pollutants["o3"] = calc_aqi(entity["o3"]["value"], [
            (0, 100, 0, 100),
            (101, 160, 101, 150),
        ])

    if "no2" in entity:
        pollutants["no2"] = calc_aqi(entity["no2"]["value"], [
            (0, 100, 0, 100),
            (101, 200, 101, 150),
        ])

    pollutants = {k: v for k, v in pollutants.items() if v is not None}

    return max(pollutants, key=pollutants.get) if pollutants else "unknown"


# ---------------------------------------------------------
# FIWARE FETCH
# ---------------------------------------------------------
def fetch_fiware(type_name):
    delay = 1
    while True:
        try:
            r = requests.get(FIWARE_URL, params={"type": type_name}, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"ERRO FIWARE ({type_name}): {e}. Nova tentativa em {delay}s…")
            time.sleep(delay)
            delay = min(delay * 2, 60)


# ---------------------------------------------------------
# PUBLICAÇÃO MQTT (SEM DISCOVERY)
# ---------------------------------------------------------
def publish_values(client, entity_id, entity, local_name, sensor_base, sensor_fields, extra_fields, include_aqi=False):
    for field in sensor_fields:
        if field in entity:
            value = entity[field]["value"]

            if field == "relativeHumidity":
                try:
                    if value <= 1:
                        value = round(value * 100, 1)
                except:
                    pass

            client.publish(f"{sensor_base}/{entity_id}/{field}", value)

    client.publish(f"{sensor_base}/{entity_id}/local", local_name)

    if "dateObserved" in entity:
        client.publish(f"{sensor_base}/{entity_id}/dateObserved", entity["dateObserved"]["value"])

    now = datetime.now(timezone.utc).isoformat()
    client.publish(f"{sensor_base}/{entity_id}/last_mqtt_update", now)

    if include_aqi:
        aqi = compute_aqi(entity)
        if aqi is not None:
            client.publish(f"{sensor_base}/{entity_id}/aqi", aqi)

        main = compute_main_pollutant(entity)
        client.publish(f"{sensor_base}/{entity_id}/main_pollutant", main)


# ---------------------------------------------------------
# PARSE DATETIME
# ---------------------------------------------------------
def parse_fiware_datetime(dt):
    if dt.endswith("Z"):
        dt = dt[:-1] + "+00:00"

    dt = dt.replace(".00+", "+").replace(".000+", "+").replace(".0+", "+")

    try:
        return datetime.fromisoformat(dt)
    except:
        from dateutil import parser
        return parser.parse(dt)


# ---------------------------------------------------------
# LISTAR ESTAÇÕES
# ---------------------------------------------------------
def list_stations():
    print("A obter lista de estações FIWARE…")

    aq = fetch_fiware("AirQualityObserved")
    wt = fetch_fiware("WeatherObserved")


    names = []

    for entity in aq + wt:
        local_name = get_station_name(entity)
        names.append(local_name)

    names = sorted(set(names))

    print("\nEstações disponíveis:\n")
    for name in names:
        print(f" - {name}")

    print("\nUse estes nomes com --stations ou --exclude.\n")

# ---------------------------------------------------------
# Normalize names
# ---------------------------------------------------------
import re
import unicodedata

def normalize_station_name(name):
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

# ---------------------------------------------------------
# LOOP AIR QUALITY
# ---------------------------------------------------------
def loop_airquality(client, args):
    SENSOR_FIELDS_AQ = {
        "co": {},
        "no2": {},
        "o3": {},
        "pm1": {},
        "pm10": {},
        "pm25": {},
        "temperature": {},
    }

    EXTRA_FIELDS_AQ = {
        "local": {},
        "dateObserved": {},
        "last_mqtt_update": {},
        "aqi": {},
        "main_pollutant": {},
        "latitude": {},
        "longitude": {}
    }

    SENSOR_BASE_AQ = "fiware/airquality"

    while True:
        data = fetch_fiware("AirQualityObserved")
        print(f"INFO: {len(data)} entidades AirQuality.")

        name_counts = {}

        for entity in data:
            lon, lat = entity["location"]["value"]["coordinates"]
            local_name = get_station_name(entity)

            if not station_allowed(local_name, args):
                print(f"AVISO: Estação '{local_name}' filtrada.")
                continue

            safe_local = normalize_station_name(local_name)
            count = name_counts.get(safe_local, 0) + 1
            name_counts[safe_local] = count

            entity_id = f"{safe_local}_{count}" if count > 1 else safe_local

            date_obs_str = entity.get("dateObserved", {}).get("value")
            if date_obs_str:
                try:
                    date_obs = parse_fiware_datetime(date_obs_str)
                    age = datetime.now(timezone.utc) - date_obs

                    if age > timedelta(days=1):
                        print(f"AVISO: {entity_id} ignorado (observação antiga).")
                        continue

                except Exception as e:
                    print(f"ERRO: Data inválida em AirQuality: {e}")

            print(f"INFO: Estação '{local_name}' → ID '{entity_id}'")

            publish_values(client, entity_id, entity, local_name, SENSOR_BASE_AQ,
                           SENSOR_FIELDS_AQ, EXTRA_FIELDS_AQ, include_aqi=True)

            client.publish(f"{SENSOR_BASE_AQ}/{entity_id}/latitude", lat)
            client.publish(f"{SENSOR_BASE_AQ}/{entity_id}/longitude", lon)

        time.sleep(60)


# ---------------------------------------------------------
# LOOP WEATHER
# ---------------------------------------------------------
def loop_weather(client, args):
    SENSOR_FIELDS_W = {
        "precipitation": {},
        "temperature": {},
        "windSpeed": {},
        "relativeHumidity": {},
        "uv": {},
    }

    EXTRA_FIELDS_W = {
        "local": {},
        "dateObserved": {},
        "last_mqtt_update": {},
        "latitude": {},
        "longitude": {}
    }

    SENSOR_BASE_W = "fiware/weather"

    while True:
        data = fetch_fiware("WeatherObserved")
        print(f"INFO: {len(data)} entidades Weather.")

        name_counts = {}

        for entity in data:
            lon, lat = entity["location"]["value"]["coordinates"]
            local_name = get_station_name(entity)

            if not station_allowed(local_name, args):
                print(f"AVISO: Estação '{local_name}' filtrada.")
                continue

            safe_local = local_name.lower().replace(" ", "_")
            count = name_counts.get(safe_local, 0) + 1
            name_counts[safe_local] = count

            entity_id = f"{safe_local}_{count}" if count > 1 else safe_local

            date_obs_str = entity.get("dateObserved", {}).get("value")
            if date_obs_str:
                try:
                    date_obs = parse_fiware_datetime(date_obs_str)
                    age = datetime.now(timezone.utc) - date_obs

                    if age > timedelta(days=1):
                        print(f"AVISO: {entity_id} ignorado (observação antiga).")
                        continue

                except Exception as e:
                    print(f"ERRO: Data inválida em Weather: {e}")

            print(f"INFO: Estação '{local_name}' → ID '{entity_id}'")

            publish_values(client, entity_id, entity, local_name, SENSOR_BASE_W,
                           SENSOR_FIELDS_W, EXTRA_FIELDS_W)

            client.publish(f"{SENSOR_BASE_W}/{entity_id}/latitude", lat)
            client.publish(f"{SENSOR_BASE_W}/{entity_id}/longitude", lon)

        time.sleep(60)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    args = parse_args()

    if args.list_stations:
        list_stations()
        exit(0)

    client = mqtt_init(args)

    threading.Thread(target=loop_airquality, args=(client, args), daemon=True).start()
    threading.Thread(target=loop_weather, args=(client, args), daemon=True).start()

    while True:
        time.sleep(10)
