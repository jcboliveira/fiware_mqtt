fiware-mqtt — Bridge FIWARE → MQTT

O fiware-mqtt é um utilitário em Python para recolha e normalização de dados FIWARE provenientes da Urban Platform do Porto Digital, suportando as entidades AirQualityObserved e WeatherObserved. Converte as observações em métricas estruturadas e publica-as em tópicos MQTT organizados por estação. Funciona em modo contínuo, pode utilizar o Home Assistant Discovery e é configurado exclusivamente por argumentos de linha de comandos.

1 - Instalação

1.1) Instalar Python, pip e venv:

apt update  
apt install -y python3 python3-pip python3-venv

1.2) Criar diretório do projeto:

mkdir ~/fiware-mqtt  
cd ~/fiware-mqtt

1.3) Criar ambiente virtual:
python3 -m venv env

1.4) Ativar ambiente virtual:
source env/bin/activate

1.5) Instalar dependências:
pip install requests paho-mqtt python-dateutil

1.6) Criar ficheiro principal:
nano fiware-mqtt  
(colar o script completo)

1.7) Tornar executável:
chmod +x fiware-mqtt

2) Opcional para arranque automático
2.1) Criar serviço systemd:
nano /etc/systemd/system/fiware.service

    [Unit]
    Description=FIWARE ~F~R MQTT Bridge
    After=network.target

    [Service]
    Type=simple
    Environment="VIRTUAL_ENV=/root/fiware-ha"
    Environment="PATH=/root/fiware-ha/bin:/usr/bin:/bin"
    ExecStart=/root/fiware-ha/bin/python /root/fiware-ha/fiware_mqtt.py
    WorkingDirectory=/root/fiware-ha
    Restart=always
    RestartSec=5
    User=root

    [Install]
    WantedBy=multi-user.target

2.2) Ativar:
systemctl daemon-reload
systemctl enable fiware
systemctl start fiware

2.3) Logs:
 journalctl -u fiware -f


Exemplo com MQTT configurado:
./fiware-mqtt --mqtt-host 192.168.1.50 --mqtt-user admin --mqtt-pass segredo 

Exemplo com MQTT configurado e envio com auto discovery para o Home assistant:
./fiware-mqtt --mqtt-host 192.168.1.50 --mqtt-user admin --mqtt-pass segredo --homeassistant-discovery

Exemplo com filtros de estações:
./fiware-mqtt --stations "Paranhos 4,Ramalde" 

2) Execução

2.1) saber as estações existentes. Definir broker MQTT e credenciais devolve lista de estações

./fiware-mqtt --mqtt-host xx.xx.xx.xx --mqtt-user admin --mqtt-pass segredo --list-stations

2.2)incluir só determinadas estações. Definir broker MQTT e credenciais, lista de estações

./fiware-mqtt --mqtt-host xx.xx.xx.xx --mqtt-user admin --mqtt-pass segredo  -stations "estação 1,estação 2"

2.3) Excluir estações.Definir broker MQTT e credenciais,  lista de estações

./fiware-mqtt -mqtt-host 192.168.1.50 --mqtt-user admin --mqtt-pass segredo --exclude "estação 1,estação 2" 

2.4) exemplos
./fiware-mqtt --list-stations --mqtt-host 192.168.1.120 --mqtt-user mqtt --mqtt-pass mqtt 

 - Pólo Asprela
 - Aliados


./fiware-mqtt --stations "Paranhos, Paranhos3" --mqtt-host 192.168.1.120 --mqtt-user mqtt --mqtt-pass mqtt 

2) Execução

Tópicos MQTT publicados

AirQualityObserved:

fiware/airquality/<estacao>/pm25
fiware/airquality/<estacao>/pm10
fiware/airquality/<estacao>/o3
fiware/airquality/<estacao>/no2
fiware/airquality/<estacao>/temperature
fiware/airquality/<estacao>/local
fiware/airquality/<estacao>/dateObserved
fiware/airquality/<estacao>/last_mqtt_update
fiware/airquality/<estacao>/aqi
fiware/airquality/<estacao>/main_pollutant

WeatherObserved:

fiware/weather/<estacao>/temperature
fiware/weather/<estacao>/windSpeed
fiware/weather/<estacao>/relativeHumidity
fiware/weather/<estacao>/precipitation
fiware/weather/<estacao>/uv
fiware/weather/<estacao>/local
fiware/weather/<estacao>/dateObserved
fiware/weather/<estacao>/last_mqtt_update

Notas técnicas
Observações FIWARE com mais de 24 horas são descartadas.

O ciclo de publicação opera a cada 60 segundos.

O programa é agnóstico ao consumidor MQTT e pode implementar autodiscovery.

Licença
Uso livre para fins pessoais, académicos ou experimentais.