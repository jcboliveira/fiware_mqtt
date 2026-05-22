fiware-mqtt — Bridge FIWARE → MQTT

O fiware-mqtt é um utilitário em Python para recolha e normalização de dados FIWARE provenientes da Urban Platform do Porto Digital, suportando as entidades AirQualityObserved e WeatherObserved. Converte as observações em métricas estruturadas e publica-as em tópicos MQTT organizados por estação. Funciona em modo contínuo, não utiliza Home Assistant Discovery e é configurado exclusivamente por argumentos de linha de comandos.

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

1.8) Obter a API Key do LocationIQ (Geocoding)

1.8.1) Criar conta gratuita em:
https://locationiq.com/

1.8.2) Aceder ao painel → Dashboard → API Access Tokens

Copiar a chave do tipo "Search & Reverse Geocoding"

A key tem o formato:
pk.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

Esta key é obrigatória para converter coordenadas FIWARE em nomes de localização.

Passar a key como argumento
O script exige a key via argumento obrigatório:
--geocoding-key <API_KEY>

Exemplo simples:
./fiware-mqtt --geocoding-key pk.MINHA_KEY

Exemplo com MQTT configurado:
./fiware-mqtt --mqtt-host 192.168.1.50 --mqtt-user admin --mqtt-pass segredo --geocoding-key pk.MINHA_KEY

Exemplo com filtros de estações:
./fiware-mqtt --stations "Paranhos 4,Ramalde" --geocoding-key pk.MINHA_KEY

2) Execução

2.1) saber as estações existentes. Definir broker MQTT e credenciais, geocoding-key devolve lista de estações

./fiware-mqtt --mqtt-host xx.xx.xx.xx --mqtt-user admin --mqtt-pass segredo --geocoding-key pk.MINHA_KEY --list-stations

2.2)incluir só determinadas estações. Definir broker MQTT e credenciais, geocoding-key, lista de estações

./fiware-mqtt --mqtt-host xx.xx.xx.xx --mqtt-user admin --mqtt-pass segredo --geocoding-key pk.MINHA_KEY -stations "estação 1,estação 2"

2.3) Excluir estações.Definir broker MQTT e credenciais, geocoding-key, lista de estações

./fiware-mqtt -mqtt-host 192.168.1.50 --mqtt-user admin --mqtt-pass segredo --geocoding-key pk.MINHA_KEY --exclude "estação 1,estação 2" 

2.4) exemplos
./fiware-mqtt --list-stations --mqtt-host 192.168.1.120 --mqtt-user mqtt --mqtt-pass mqtt --geocoding-key pk.ef998abd3c

 - Bonfim
 - Bonfim 3
 - Cedofeita
 - Nevogilde
 - Nevogilde 2
 - Nevogilde 3
 - Nevogilde 4
 - Paranhos
 - Paranhos 2
 - Paranhos 3
 - Paranhos 4
 - Porto
 - Santo Ildefonso

./fiware-mqtt --stations "Paranhos, Paranhos3" --mqtt-host 192.168.1.120 --mqtt-user mqtt --mqtt-pass mqtt --geocoding-key pk.ef998abd3c

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

Os nomes das estações são resolvidos via geocoding (LocationIQ) e armazenados em cache.

O ciclo de publicação opera a cada 60 segundos.

O programa é agnóstico ao consumidor MQTT e não implementa autodiscovery.

Licença
Uso livre para fins pessoais, académicos ou experimentais.