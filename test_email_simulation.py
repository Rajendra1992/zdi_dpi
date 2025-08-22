#!/usr/bin/env python3
"""
Email Simulation Script for Snowflake OpenFlow POC Testing

This script simulates sending emails with CSV attachments to test
the email-to-Snowflake OpenFlow pipeline.
"""

import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import csv
import tempfile
from datetime import datetime
import random

class EmailSimulator:
    def __init__(self, smtp_server, smtp_port, username, password, use_tls=True):
        """
        Initialize the email simulator
        
        Args:
            smtp_server (str): SMTP server hostname
            smtp_port (int): SMTP server port
            username (str): Email username
            password (str): Email password
            use_tls (bool): Whether to use TLS encryption
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
    
    def create_test_csv(self, filename, num_records=10):
        """
        Create a test CSV file with sample employee data
        
        Args:
            filename (str): Output filename
            num_records (int): Number of records to generate
        """
        departments = ['Engineering', 'Marketing', 'Sales', 'HR', 'Finance', 'IT', 'Operations']
        statuses = ['Active', 'Inactive', 'Pending']
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['employee_id', 'first_name', 'last_name', 'email', 
                         'department', 'hire_date', 'salary', 'status']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for i in range(1, num_records + 1):
                employee_id = 2000 + i
                first_name = f"TestUser{i:03d}"
                last_name = f"LastName{i:03d}"
                email = f"test.user{i:03d}@testcompany.com"
                department = random.choice(departments)
                hire_date = f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                salary = random.randint(50000, 120000)
                status = random.choice(statuses)
                
                writer.writerow({
                    'employee_id': employee_id,
                    'first_name': first_name,
                    'last_name': last_name,
                    'email': email,
                    'department': department,
                    'hire_date': hire_date,
                    'salary': salary,
                    'status': status
                })
        
        print(f"Created test CSV file: {filename} with {num_records} records")
    
    def send_test_email(self, to_email, csv_file_path, subject_prefix="OpenFlow Test"):
        """
        Send a test email with CSV attachment
        
        Args:
            to_email (str): Recipient email address
            csv_file_path (str): Path to CSV file to attach
            subject_prefix (str): Subject line prefix
        """
        try:
            # Create message container
            msg = MIMEMultipart()
            
            # Set email headers
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg['From'] = self.username
            msg['To'] = to_email
            msg['Subject'] = f"{subject_prefix} - Employee Data Update - {timestamp}"
            
            # Email body
            body = f"""
This is a test email for the Snowflake OpenFlow POC.

Email Details:
- Sent at: {timestamp}
- Attachment: {os.path.basename(csv_file_path)}
- Purpose: Testing email-to-Snowflake pipeline

The attached CSV file contains employee data that should be processed
by the OpenFlow pipeline and loaded into the Snowflake EMPLOYEE_DATA table.

Please verify that:
1. Email is received and processed by GetEmail processor
2. CSV attachment is extracted successfully
3. Data is converted and validated properly
4. Records are loaded into Snowflake table
5. Success logs are generated

This is an automated test email.
            """.strip()
            
            # Attach body to email
            msg.attach(MIMEText(body, 'plain'))
            
            # Attach CSV file
            with open(csv_file_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {os.path.basename(csv_file_path)}'
            )
            
            msg.attach(part)
            
            # Create SMTP session
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            
            if self.use_tls:
                server.starttls()  # Enable security
            
            server.login(self.username, self.password)
            
            # Send email
            text = msg.as_string()
            server.sendmail(self.username, to_email, text)
            server.quit()
            
            print(f"Test email sent successfully to {to_email}")
            print(f"Subject: {msg['Subject']}")
            print(f"Attachment: {os.path.basename(csv_file_path)}")
            
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            raise
    
    def run_test_scenarios(self, target_email):
        """
        Run multiple test scenarios
        
        Args:
            target_email (str): Target email for testing
        """
        test_scenarios = [
            {"name": "Small Dataset", "records": 10, "filename": "small_test_data.csv"},
            {"name": "Medium Dataset", "records": 100, "filename": "medium_test_data.csv"},
            {"name": "Large Dataset", "records": 1000, "filename": "large_test_data.csv"},
            {"name": "Edge Case - Single Record", "records": 1, "filename": "single_record.csv"},
            {"name": "Edge Case - Empty Data", "records": 0, "filename": "empty_data.csv"}
        ]
        
        print("Starting OpenFlow Email Test Scenarios...")
        print("="*50)
        
        for i, scenario in enumerate(test_scenarios, 1):
            print(f"\nScenario {i}: {scenario['name']}")
            print("-" * 30)
            
            # Create temporary CSV file
            temp_file = os.path.join(tempfile.gettempdir(), scenario['filename'])
            
            try:
                self.create_test_csv(temp_file, scenario['records'])
                
                # Send email with attachment
                subject = f"OpenFlow Test Scenario {i} - {scenario['name']}"
                self.send_test_email(target_email, temp_file, subject)
                
                print(f"✓ Scenario {i} completed successfully")
                
            except Exception as e:
                print(f"✗ Scenario {i} failed: {str(e)}")
            
            finally:
                # Clean up temporary file
                if os.path.exists(temp_file):
                    os.remove(temp_file)
        
        print("\n" + "="*50)
        print("All test scenarios completed!")
        print("\nNext Steps:")
        print("1. Monitor NiFi OpenFlow canvas for processing")
        print("2. Check Snowflake EMPLOYEE_DATA table for loaded records")
        print("3. Review NiFi logs for any errors or warnings")
        print("4. Verify email notifications if any failures occur")

def main():
    """
    Main function to run the email simulation
    """
    print("Snowflake OpenFlow Email Simulation Tool")
    print("="*40)
    
    # Configuration (update these values for your environment)
    SMTP_CONFIG = {
        'smtp_server': 'smtp.gmail.com',  # Update for your email provider
        'smtp_port': 587,
        'username': 'your-test-email@gmail.com',  # Update with your email
        'password': 'your-app-password',  # Update with your app password
        'use_tls': True
    }
    
    TARGET_EMAIL = 'nifi-test@yourdomain.com'  # Update with target email
    
    print("\nConfiguration:")
    print(f"SMTP Server: {SMTP_CONFIG['smtp_server']}:{SMTP_CONFIG['smtp_port']}")
    print(f"From Email: {SMTP_CONFIG['username']}")
    print(f"Target Email: {TARGET_EMAIL}")
    print(f"TLS Enabled: {SMTP_CONFIG['use_tls']}")
    
    # Validate configuration
    if 'your-test-email@gmail.com' in SMTP_CONFIG['username']:
        print("\n⚠️  WARNING: Please update the email configuration before running!")
        print("Update the SMTP_CONFIG and TARGET_EMAIL variables in the script.")
        return
    
    try:
        # Create email simulator
        simulator = EmailSimulator(**SMTP_CONFIG)
        
        # Run test scenarios
        simulator.run_test_scenarios(TARGET_EMAIL)
        
    except Exception as e:
        print(f"\n❌ Error running email simulation: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Verify SMTP server settings")
        print("2. Check email credentials and app password")
        print("3. Ensure firewall allows SMTP traffic")
        print("4. Verify target email address is correct")

if __name__ == "__main__":
    main()