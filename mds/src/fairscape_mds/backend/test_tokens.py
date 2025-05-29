import requests
import json

# --- Configuration ---
FAIRSCAPE_API_BASE_URL = "http://localhost:8080" # Adjust if your API prefix is different
LOGIN_EMAIL = "max_headroom@example.org"
LOGIN_PASSWORD = "testpassword"

TOKEN_UID = "my-test-site-token"
TOKEN_VALUE = "faketoken12345!"
TOKEN_ENDPOINT_URL = "https://test.com"
TOKEN_DESCRIPTION = "This is a fake description for a test token"
# --- End Configuration ---

def login(email, password):
    """Logs in to the Fairscape API and returns the access token."""
    login_url = f"{FAIRSCAPE_API_BASE_URL}/login"
    data = {
        "username": email, # FastAPI's OAuth2PasswordRequestForm expects 'username'
        "password": password
    }
    try:
        response = requests.post(login_url, data=data)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        login_data = response.json()
        access_token = login_data.get("access_token")
        if not access_token:
            print("Error: 'access_token' not found in login response.")
            print("Response:", login_data)
            return None
        print(f"Successfully logged in. Access Token: {access_token[:20]}...") # Print partial token
        return access_token
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error during login: {http_err}")
        print("Response content:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"Request error during login: {req_err}")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON response from login.")
        print("Response text:", response.text)
    return None

def create_api_token(access_token, token_uid, token_value, endpoint_url, description):
    """Creates a new API token using the Fairscape API."""
    create_token_url = f"{FAIRSCAPE_API_BASE_URL}/profile/credentials"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "tokenUID": token_uid,
        "tokenValue": token_value,
        "endpointURL": endpoint_url,
        "description": description
    }
    try:
        response = requests.post(create_token_url, headers=headers, json=payload)
        response.raise_for_status()
        print("\nAPI Token creation successful!")
        print("Response:", response.json())
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"\nHTTP error during token creation: {http_err}")
        print("Response content:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"\nRequest error during token creation: {req_err}")
    except json.JSONDecodeError:
        print("\nError: Could not decode JSON response from token creation.")
        print("Response text:", response.text)
    return None

def get_api_tokens(access_token):
    """Gets all API tokens for the current user."""
    get_tokens_url = f"{FAIRSCAPE_API_BASE_URL}/profile/credentials"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response = requests.get(get_tokens_url, headers=headers)
        response.raise_for_status()
        print("\nSuccessfully retrieved API tokens:")
        tokens = response.json()
        if tokens:
            for token in tokens:
                # Mask the token value for printing
                masked_value = token.get('tokenValue', '')[:3] + '...' + token.get('tokenValue', '')[-3:] if token.get('tokenValue') else 'N/A'
                print(f"  UID: {token.get('tokenUID')}, Endpoint: {token.get('endpointURL')}, Value: {masked_value}, Desc: {token.get('description')}")
        else:
            print("  No tokens found.")
        return tokens
    except requests.exceptions.HTTPError as http_err:
        print(f"\nHTTP error retrieving tokens: {http_err}")
        print("Response content:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"\nRequest error retrieving tokens: {req_err}")
    except json.JSONDecodeError:
        print("\nError: Could not decode JSON response from get tokens.")
        print("Response text:", response.text)
    return None

def delete_api_token(access_token, token_uid):
    """Deletes a specific API token."""
    delete_url = f"{FAIRSCAPE_API_BASE_URL}/profile/credentials/{token_uid}"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    try:
        response = requests.delete(delete_url, headers=headers)
        response.raise_for_status()
        print(f"\nSuccessfully deleted token with UID: {token_uid}")
        print("Response:", response.json())
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"\nHTTP error during token deletion: {http_err}")
        print("Response content:", response.text)
    except requests.exceptions.RequestException as req_err:
        print(f"\nRequest error during token deletion: {req_err}")
    except json.JSONDecodeError:
        print("\nError: Could not decode JSON response from token deletion.")
        print("Response text:", response.text)
    return None


if __name__ == "__main__":
    print("Attempting to log in...")
    bearer_token = login(LOGIN_EMAIL, LOGIN_PASSWORD)

    if bearer_token:
        print("\nAttempting to create an API token...")
        created_token_info = create_api_token(
            bearer_token,
            TOKEN_UID,
            TOKEN_VALUE,
            TOKEN_ENDPOINT_URL,
            TOKEN_DESCRIPTION
        )

        if created_token_info:
            print("\nVerifying by listing all tokens...")
            get_api_tokens(bearer_token)

            # Example of how to delete the token (optional cleanup)
            # print(f"\nAttempting to delete the token with UID: {TOKEN_UID}...")
            # delete_api_token(bearer_token, TOKEN_UID)
            #
            # print("\nVerifying deletion by listing all tokens again...")
            # get_api_tokens(bearer_token)
        else:
            print("\nToken creation failed, cannot proceed to list or delete.")
    else:
        print("\nLogin failed. Cannot proceed with API token operations.")