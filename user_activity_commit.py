import requests
import os
import datetime
import pandas as pd
# Import relativedelta for accurate month calculation
from dateutil.relativedelta import relativedelta 
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
GITLAB_URL = os.getenv("GITLAB_URL")
PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")

# API Endpoints
USERS_API_URL = f"{GITLAB_URL}/api/v4/users"
EVENTS_PER_PAGE = 100 
USERS_PER_PAGE = 100

# Headers for authentication
HEADERS = {
    "Private-Token": PRIVATE_TOKEN
}

# --- Date Calculation ---
MONTHS_TO_GO_BACK = 6
TODAY = datetime.date.today()

# Use relativedelta for precise month subtraction
end_date = TODAY + datetime.timedelta(days=1) # Events up to tomorrow
start_date = TODAY - relativedelta(months=MONTHS_TO_GO_BACK) 

# Convert to ISO 8601 format required by the API
AFTER_DATE = start_date.strftime('%Y-%m-%d')
BEFORE_DATE = end_date.strftime('%Y-%m-%d')

print(f"Counting 'pushed to' events from {AFTER_DATE} to {BEFORE_DATE}...")

# --- Helper Functions ---

def get_all_users():
    """Retrieves all active user IDs and usernames from GitLab, handling pagination."""
    # ... (function body remains the same as your original script)
    all_users = []
    page = 1
    
    while True:
        params = {
            'per_page': USERS_PER_PAGE,
            'page': page,
            'active': True 
        }
        
        try:
            response = requests.get(USERS_API_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            users = response.json()
            if not users:
                break
            
            for user in users:
                all_users.append({
                    'id': user.get('id'),
                    'username': user.get('username')
                })
            
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching users: {e}")
            return None
    
    print(f"Found {len(all_users)} active users.")
    return all_users

def get_user_push_events(user_id, username):
    """Retrieves detailed 'pushed to' events for a single user."""
    all_events = []
    page = 1
    
    while True:
        # API endpoint for a specific user's events
        USER_EVENTS_URL = f"{GITLAB_URL}/api/v4/users/{user_id}/events"
        params = {
            # Note: We still use 'action': 'pushed' for a preliminary filter
            'action': 'pushed', 
            'after': AFTER_DATE,
            'before': BEFORE_DATE,
            'per_page': EVENTS_PER_PAGE,
            'page': page
        }
        
        try:
            response = requests.get(USER_EVENTS_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            events = response.json()
            if not events:
                break 

            for event in events:
                if event.get('action_name') == 'pushed to':
                    # Extract the detailed push activity
                    event_data = {
                        'Username': username,
                        # The 'push_data' is used for the detailed message
                        'Activity Name': event.get('push_data', {}).get('action', event.get('action_name')),
                        'Project Name': event.get('project_id'), # Keep ID for later lookup/joining if needed
                        'Project Path': event.get('project_path'),
                        'Commit Count': event.get('push_data', {}).get('commit_count'),
                        'Pushed To Branch': event.get('push_data', {}).get('ref'),
                        'Event Date': event.get('created_at')
                    }
                    all_events.append(event_data)
            
            page += 1
            
            if len(events) < EVENTS_PER_PAGE:
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching events for user {username} (ID: {user_id}): {e}")
            break
            
    return all_events

def save_to_excel(summary_data, detailed_data):
    """Saves the push event summary and detailed data to a multi-sheet Excel file."""
    try:
        # Create DataFrames
        summary_df = pd.DataFrame(summary_data)
        detailed_df = pd.DataFrame(detailed_data)

        # Sort summary by count
        summary_df = summary_df.sort_values(by='Push Count', ascending=False)
        
        filename = f"GitLab_Push_Report_{TODAY.strftime('%Y%m%d')}.xlsx"

        # Use ExcelWriter to write to multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            detailed_df.to_excel(writer, sheet_name='Detailed Activity', index=False)
        
        print("\n" + "=" * 60)
        print(f"🎉 Successfully saved report to {filename}")
        print("=" * 60)

    except Exception as e:
        print(f"Error saving to Excel: {e}")


# --- Main Logic ---

if __name__ == "__main__":
    if not GITLAB_URL or not PRIVATE_TOKEN:
        print("Error: GITLAB_URL or GITLAB_PRIVATE_TOKEN not found in environment variables.")
    else:
        user_list = get_all_users()
        
        if user_list:
            # Store data for the final report
            summary_results = []
            detailed_activity = []
            total_pushes = 0
            
            print("-" * 50)
            print("Processing events for each user...")
            
            for user in user_list:
                user_id = user['id']
                username = user['username']
                
                # Fetch detailed push events
                events = get_user_push_events(user_id, username)
                
                count = len(events)
                if count > 0:
                    # Append detailed events to the list
                    detailed_activity.extend(events)
                    
                    # Store summary data
                    summary_results.append({
                        'Username': username,
                        'Push Count': count
                    })
                    total_pushes += count
                    print(f"-> {username:<20}: {count} pushes")

            # --- Output Results ---
            print("\n" + "=" * 50)
            print(f"✅ GitLab User Push Event Summary ({MONTHS_TO_GO_BACK} Months)")
            print("=" * 50)
            
            # Print console summary
            for item in sorted(summary_results, key=lambda x: x['Push Count'], reverse=True):
                print(f"{item['Username']:<25} {item['Push Count']}")
                    
            print("-" * 50)
            print(f"Total Unique Contributors: {len(summary_results)}")
            print(f"Total Push Events:         {total_pushes}")
            print("=" * 50)
            
            # Save the data to Excel
            if detailed_activity:
                save_to_excel(summary_results, detailed_activity)