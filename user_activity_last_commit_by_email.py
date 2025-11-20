import requests
import os
import datetime
from dotenv import load_dotenv
import pandas as pd 

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
GITLAB_URL = os.getenv("GITLAB_URL")
PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")

# NEW FILTER CRITERIA
EMAIL_FILTER_SUBSTRING = "kestrl"

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
end_date = datetime.date.today() + datetime.timedelta(days=1) 
start_date = end_date - datetime.timedelta(days=MONTHS_TO_GO_BACK * 30) 

# Convert to ISO 8601 format required by the API
AFTER_DATE = start_date.strftime('%Y-%m-%d')
BEFORE_DATE = end_date.strftime('%Y-%m-%d')
OUTPUT_FILENAME = f"gitlab_push_summary_kestrl_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

print(f"Counting 'pushed to' events for users containing '{EMAIL_FILTER_SUBSTRING}' in their email from {AFTER_DATE} to {BEFORE_DATE}...")

# --- Helper Functions ---

def get_all_users():
    """
    Retrieves all active user IDs and usernames from GitLab, handling pagination,
    and filters them based on the EMAIL_FILTER_SUBSTRING.
    """
    all_users = []
    page = 1
    
    while True:
        params = {
            'per_page': USERS_PER_PAGE,
            'page': page,
            'active': True # Only process active users
        }
        
        try:
            response = requests.get(USERS_API_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            users = response.json()
            if not users:
                break # No more users
            
            for user in users:
                # --- NEW FILTER LOGIC ---
                user_email = user.get('email', '')
                if EMAIL_FILTER_SUBSTRING.lower() in user_email.lower():
                    all_users.append({
                        'id': user.get('id'),
                        'username': user.get('username'),
                        'email': user_email # Include email for clarity in final output
                    })
                # -------------------------
            
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching users: {e}")
            return None
    
    print(f"Found {len(all_users)} active users matching the email filter.")
    return all_users

# --- (get_user_push_count remains the same) ---

def get_user_push_count(user_id, username):
    """
    Counts 'pushed to' events for a single user and records the details 
    of the most recent push.
    
    Returns: (push_count, last_push_date, last_push_commit_sha)
    """
    push_count = 0
    page = 1
    
    last_push_datetime = datetime.datetime(1, 1, 1, tzinfo=datetime.timezone.utc)
    last_push_commit_sha = "N/A"
    
    while True:
        USER_EVENTS_URL = f"{GITLAB_URL}/api/v4/users/{user_id}/events"
        params = {
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
                    push_count += 1
                    
                    current_event_datetime = datetime.datetime.fromisoformat(
                        event['created_at'].replace('Z', '+00:00')
                    )
                    
                    if current_event_datetime > last_push_datetime:
                        last_push_datetime = current_event_datetime
                        push_data = event.get('push_data', {})
                        last_push_commit_sha = push_data.get('commit_to', 'N/A')
                        
            
            page += 1
            
            if len(events) < EVENTS_PER_PAGE:
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching events for user {username} (ID: {user_id}): {e}")
            break
            
    formatted_date = last_push_datetime.strftime('%Y-%m-%d %H:%M:%S') if push_count > 0 else "N/A"
    
    return (push_count, formatted_date, last_push_commit_sha)

# --- (save_to_xlsx updated to handle email) ---

def save_to_xlsx(results_data, filename):
    """Converts the results dictionary to a Pandas DataFrame and saves it to an XLSX file."""
    try:
        # Convert dictionary of dictionaries into a DataFrame
        df = pd.DataFrame.from_dict(results_data, orient='index')
        
        # Reset index to make 'Username' a column
        df = df.reset_index().rename(columns={'index': 'Username'})
        
        # Rename columns for clarity
        df.columns = ['Username', 'Email', 'Push Count', 'Last Push Date/Time', 'Last Commit SHA']
        
        # Sort the DataFrame by 'Push Count' descending
        df = df.sort_values(by='Push Count', ascending=False)
        
        # Write to Excel file
        df.to_excel(filename, index=False, sheet_name='GitLab Push Summary')
        print(f"\n✨ Successfully saved results to: **{filename}**")
    except Exception as e:
        print(f"\n❌ Error saving to XLSX file: {e}")

# --- Main Logic ---

if __name__ == "__main__":
    # Check for pandas requirement
    try:
        pd.DataFrame()
    except NameError:
        print("\n**CRITICAL ERROR:** The 'pandas' library is required to save to XLSX.")
        print("Please install it: `pip install pandas openpyxl`")
        exit()

    if not GITLAB_URL or not PRIVATE_TOKEN:
        print("Error: GITLAB_URL or GITLAB_PRIVATE_TOKEN not found in .env file.")
    else:
        user_list = get_all_users()
        
        if user_list:
            results_details = {} 
            total_pushes = 0
            
            print("-" * 75)
            print("Processing events and finding latest push activity for each user...")
            
            for user in user_list:
                user_id = user['id']
                username = user['username']
                user_email = user['email'] # Retrieve email here
                
                # Fetch count and last push details
                count, last_date, last_commit = get_user_push_count(user_id, username)
                
                # Store only users who have push events
                if count > 0:
                    results_details[username] = {
                        'email': user_email, # Added email to results
                        'count': count,
                        'last_date': last_date,
                        'last_commit': last_commit
                    }
                    total_pushes += count
                    print(f"-> {username:<20} ({user_email}): {count} pushes, Last: {last_date} ({last_commit[:8]}...)")

            # --- Output Results to Console and XLSX ---
            
            print("\n" + "=" * 110)
            print(f"✅ GitLab Push Event Summary for Users with '{EMAIL_FILTER_SUBSTRING}' ({MONTHS_TO_GO_BACK} Months)")
            print("=" * 110)
            
            # Sort the results for console output
            sorted_results = sorted(
                results_details.items(), 
                key=lambda item: item[1]['count'], 
                reverse=True
            )
            
            # Print Header
            print(f"{'Username':<20} {'Email':<30} {'Pushes':<8} {'Last Push Date':<20} {'Last Commit SHA (First 8)':<20}")
            print("-" * 110)

            for username, data in sorted_results:
                print(f"{username:<20} {data['email']:<30} {data['count']:<8} {data['last_date']:<20} {data['last_commit'][:8]:<20}")
                
            print("-" * 110)
            print(f"Total Unique Contributors (kestrl): {len(results_details)}")
            print(f"Total Push Events (kestrl): {total_pushes}")
            print("=" * 110)
            
            # XLSX Output
            if results_details:
                save_to_xlsx(results_details, OUTPUT_FILENAME)