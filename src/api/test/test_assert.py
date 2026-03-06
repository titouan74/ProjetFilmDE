import os
import sys
import time
import socket
import subprocess
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# définition de l'adresse de l'API
api_address = '127.0.0.1'
# port de l'API
api_port = 8000

# auth depuis .env
load_dotenv("src/api/.env")
api_user = os.getenv("API_USER")
api_password = os.getenv("API_PASSWORD")
auth = HTTPBasicAuth(api_user, api_password)

# Format de sortie des tests
output = '''
============================
    {test_name} test
============================

request done at "/{endpoint}"

Expected result = {expected_status}
Actual result = {status_code}

==>  {test_status}

'''

# Fonction d'enregistrement des logs
def log_test(output_str: str):
    if os.environ.get('LOG') == '1':
        with open('api_test.log', 'a') as file:
            file.write(output_str)

# Test de la route health
def test_health():
    r = requests.get(
        url='http://{address}:{port}/health'.format(address=api_address, port=api_port),auth=auth
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Health", endpoint="health", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 200

# Test de la route predict
def test_predict_existing_movie():
    r = requests.post(
        url='http://{address}:{port}/predict'.format(address=api_address, port=api_port),auth=auth,
        json={
            "movie_title": "Inception",
            "target": "revenue",
            "model": "xgb"
        }
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Predict existing movie", endpoint="predict", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 200

# Test de la route predict avec un film non existant
def test_predict_nonexistent_movie():
    r = requests.post(
        url='http://{address}:{port}/predict'.format(address=api_address, port=api_port),auth=auth,
        json={
            "movie_title": "Nonexistent Movie",
            "target": "revenue",
            "model": "xgb"
        }
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 404:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Predict Nonexistent Movie", endpoint="predict", expected_status=404, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 404

# Test de la route models
def test_list_models():
    r = requests.get(
        url='http://{address}:{port}/models'.format(address=api_address, port=api_port),auth=auth
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="List Models", endpoint="models", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 200




# Test de la route status
def test_status():
    r = requests.get(
        url='http://{address}:{port}/status'.format(address=api_address, port=api_port),auth=auth
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Status", endpoint="status", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

# Test de la route movies/count
def test_count_movies():
    r = requests.get(
        url='http://{address}:{port}/movies/count'.format(address=api_address, port=api_port),auth=auth
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Movies Count", endpoint="movies/count", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 200

# Test de la route movies/search avec un mot existant
def test_search_movies():
    r = requests.get(
        url='http://{address}:{port}/movies/search'.format(address=api_address, port=api_port),auth=auth,
        params={"query": "war"}
    )

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == 200:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'

    test_result = output.format(test_name="Movies Search", endpoint="movies/search", expected_status=200, status_code=status_code, test_status=test_status)
    print(test_result)

    # impression dans un fichier
    log_test(test_result)

    assert status_code == 200