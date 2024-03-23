import requests

# URL al que deseas enviar el POST request
url = 'https://advana-challenge-check-api-cr-k4hdbggvoq-uc.a.run.app/data-engineer'

# Estructura de cuerpo del POST request
data = {
    "name": "Juan José Gutiérrez Terraza",
    "mail": "juan.jose.gutierrez.terraza@gmail.com",
    "github_url": "https://github.com/dragolemguty/latam_challenge.git"
}

# Enviar el POST request
response = requests.post(url, json=data)

# Verificar la respuesta
if response.status_code == 200:
    print("POST request exitoso!")
    print("Respuesta del servidor:")
    print(response.json())
else:
    print("Error en el POST request:", response.status_code)