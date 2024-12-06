import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_email(event, context):
    """Triggered by a Google Cloud Pub/Sub message."""
    import base64

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        email_details = eval(message)
        
        message = Mail(
            from_email='shetty.navi@northeastern.edu',
            to_emails=email_details['EmailID'],
            subject='Reminder to Upgrade',
            plain_text_content='This is a reminder to upgrade your equipment as it has been over a year.')

        try:
            sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
            response = sg.send(message)
            print(f"Email sent to {email_details['EmailID']} with status code {response.status_code}")
        except Exception as e:
            print(str(e))
