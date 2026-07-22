import http.client, urllib

def send_pushover_notification(message):
    # Replace with your Pushover API token and user key
    conn = http.client.HTTPSConnection("api.pushover.net:443")
    conn.request("POST", "/1/messages.json",
        urllib.parse.urlencode({
        "token": "ag6bs6ccmb9woqcoytx6vc992g36v7",
        "user": "uxycpo6rvktpdq337iyy4pzf69ox62",
        "message": message,
        }), { "Content-type": "application/x-www-form-urlencoded" })
    conn.getresponse()

try:
    # Your main Python application code goes here
    # Example: Code that might raise an exception
    result = 10 / 0  # This will cause a ZeroDivisionError

    print(result)

except Exception as e:
    # This block executes if any exception occurs
    error_message = f"Your Python app crashed! Error: {e}"
    send_pushover_notification(error_message)
    # Optionally, re-raise the exception if you want the program to still exit
    raise e
