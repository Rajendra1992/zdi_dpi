#!/usr/bin/env python3
"""
Gmail Test Email Script for Snowflake OpenFlow POC

This script sends a test email with CSV attachment to your Gmail account
for testing the email-to-Snowflake pipeline.
"""

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime

def send_gmail_test():
    """Send test email with CSV attachment to Gmail account"""
    
    # Gmail SMTP Configuration
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    
    # Update these with your Gmail credentials
    FROM_EMAIL = "your-gmail@gmail.com"  # Your Gmail address
    FROM_PASSWORD = "your-app-password"   # Gmail App Password (not regular password)
    TO_EMAIL = "your-gmail@gmail.com"     # Same Gmail address (send to yourself)
    
    # Check if credentials are updated
    if "your-gmail@gmail.com" in FROM_EMAIL:
        print("⚠️  Please update the Gmail credentials in the script first!")
        print("1. Set FROM_EMAIL to your Gmail address")
        print("2. Set FROM_PASSWORD to your Gmail App Password")
        print("3. Set TO_EMAIL to your Gmail address")
        return False
    
    # Check if CSV file exists
    csv_file = "sample_data.csv"
    if not os.path.exists(csv_file):
        print(f"❌ CSV file '{csv_file}' not found!")
        print("Make sure the sample_data.csv file is in the same directory")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart()
        
        # Email headers
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg['From'] = FROM_EMAIL
        msg['To'] = TO_EMAIL
        msg['Subject'] = f"OpenFlow Test - Employee Data Update - {timestamp}"
        
        # Email body
        body = f"""
Hello,

This is a test email for the Snowflake OpenFlow POC.

Test Details:
- Sent at: {timestamp}
- Attachment: {csv_file}
- Purpose: Testing Gmail → Snowflake CSV pipeline

The attached CSV file contains employee data that should be:
1. Retrieved by GetEmail processor
2. Extracted by ExtractEmailAttachments processor
3. Validated by RouteOnAttribute processor
4. Loaded directly to Snowflake by PutSnowflake processor

Expected Result:
- Data should appear in DEMO_DB.PUBLIC.EMPLOYEE_DATA table
- Success logs should appear in NiFi

This is an automated test email for OpenFlow pipeline validation.

Best regards,
OpenFlow Test System
        """.strip()
        
        # Attach body
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach CSV file
        with open(csv_file, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {csv_file}'
        )
        msg.attach(part)
        
        # Send email
        print("📧 Connecting to Gmail SMTP server...")
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # Enable TLS encryption
        
        print("🔐 Authenticating with Gmail...")
        server.login(FROM_EMAIL, FROM_PASSWORD)
        
        print("📤 Sending test email...")
        text = msg.as_string()
        server.sendmail(FROM_EMAIL, TO_EMAIL, text)
        server.quit()
        
        print("✅ Test email sent successfully!")
        print(f"📧 From: {FROM_EMAIL}")
        print(f"📧 To: {TO_EMAIL}")
        print(f"📧 Subject: {msg['Subject']}")
        print(f"📎 Attachment: {csv_file}")
        print()
        print("🔍 Next Steps:")
        print("1. Check your Gmail inbox for the test email")
        print("2. Monitor NiFi OpenFlow canvas for processing")
        print("3. Watch processor queues fill up")
        print("4. Check Snowflake table for loaded data:")
        print("   SELECT * FROM DEMO_DB.PUBLIC.EMPLOYEE_DATA;")
        
        return True
        
    except smtplib.SMTPAuthenticationError:
        print("❌ Gmail authentication failed!")
        print()
        print("🔧 Troubleshooting:")
        print("1. Make sure you're using Gmail App Password (not regular password)")
        print("2. Enable 2-Step Verification in Gmail")
        print("3. Generate App Password: Google Account → Security → App passwords")
        print("4. Use the 16-character app password in this script")
        return False
        
    except Exception as e:
        print(f"❌ Error sending email: {str(e)}")
        print()
        print("🔧 Troubleshooting:")
        print("1. Check internet connection")
        print("2. Verify Gmail credentials")
        print("3. Ensure CSV file exists")
        return False

def main():
    """Main function"""
    print("=" * 60)
    print("📧 Gmail Test Email for Snowflake OpenFlow POC")
    print("=" * 60)
    print()
    
    print("📋 Pre-flight checks:")
    print("- Gmail IMAP enabled: ❓ (check Gmail settings)")
    print("- App password generated: ❓ (check Google Account security)")
    print("- NiFi OpenFlow running: ❓ (check your OpenFlow instance)")
    print("- Processors configured: ❓ (follow the processor guide)")
    print()
    
    # Send test email
    success = send_gmail_test()
    
    if success:
        print()
        print("🎉 Test email sent successfully!")
        print()
        print("⏱️  Expected Processing Time: 15-30 seconds")
        print()
        print("📊 Monitor these components:")
        print("1. Gmail INBOX - Email should be marked as read")
        print("2. NiFi Processors - Watch queue counters")
        print("3. Snowflake Table - Check for new records")
        print("4. NiFi Logs - Look for success/error messages")
        
    else:
        print()
        print("❌ Test email failed to send")
        print("Please fix the issues above and try again")

if __name__ == "__main__":
    main()