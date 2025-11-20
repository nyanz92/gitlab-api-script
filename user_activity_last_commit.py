import requests
import os
import datetime
from dotenv import load_dotenv
import pandas as pd # New Dependency!

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
GITLAB_URL = os.getenv("GITLAB_URL")
PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")

# API Endpoints
USERS_API_URL = f"{GITLAB_URL}/api/v4/users"
# Note: The API is paginated, so we use per_page=100 (max) to minimize requests
EVENTS_PER_PAGE = 100 
USERS_PER_PAGE = 100

# Headers for authentication
HEADERS = {
    "Private-Token": PRIVATE_TOKEN
}

# --- Date Calculation ---
MONTHS_TO_GO_BACK = 6
# Calculate the 'after' date (6 months ago)
end_date = datetime.date.today() + datetime.timedelta(days=1) # Get events up to tomorrow
start_date = end_date - datetime.timedelta(days=MONTHS_TO_GO_BACK * 30) 

# Convert to ISO 8601 format required by the API
AFTER_DATE = start_date.strftime('%Y-%m-%d')
BEFORE_DATE = end_date.strftime('%Y-%m-%d')
OUTPUT_FILENAME = f"gitlab_push_summary_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"

print(f"Counting 'pushed to' events from {AFTER_DATE} to {BEFORE_DATE}...")

# --- Helper Functions ---

def get_all_users():
    """Retrieves all active user IDs and usernames from GitLab, handling pagination."""
    all_users = []
    page = 1
    
    while True:
        params = {
            'per_page': USERS_PER_PAGE,
            'page': page,
            'active': True # Only count events for active users
        }
        
        try:
            response = requests.get(USERS_API_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            users = response.json()
            if not users:
                break # No more users
            
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

def get_user_push_count(user_id, username):
    """
    Counts 'pushed to' events for a single user and records the details 
    of the most recent push.
    
    Returns: (push_count, last_push_date, last_push_commit_sha)
    """
    push_count = 0
    page = 1
    
    # Initialize last push details to track the most recent one
    last_push_datetime = datetime.datetime(1, 1, 1, tzinfo=datetime.timezone.utc)
    last_push_commit_sha = "N/A"
    
    while True:
        # API endpoint for a specific user's events
        USER_EVENTS_URL = f"{GITLAB_URL}/api/v4/users/{user_id}/events"
        params = {
            'action': 'pushed', # Filter by pushed events
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
                break # No more events
            
            for event in events:
                if event.get('action_name') == 'pushed to':
                    push_count += 1
                    
                    # Check if this event is more recent than the current 'last_push_datetime'
                    current_event_datetime = datetime.datetime.fromisoformat(
                        event['created_at'].replace('Z', '+00:00')
                    )
                    
                    if current_event_datetime > last_push_datetime:
                        last_push_datetime = current_event_datetime
                        # The push event data is nested under 'push_data'
                        push_data = event.get('push_data', {})
                        last_push_commit_sha = push_data.get('commit_to', 'N/A')
                        
            
            page += 1
            
            # Optimization: If the number of events returned is less than the max per page,
            # we assume we've hit the end and break early.
            if len(events) < EVENTS_PER_PAGE:
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching events for user {username} (ID: {user_id}): {e}")
            break
            
    # Format the date for output, or return "N/A" if no pushes were found
    formatted_date = last_push_datetime.strftime('%Y-%m-%d %H:%M:%S') if push_count > 0 else "N/A"
    
    return (push_count, formatted_date, last_push_commit_sha)

# --- New Function: Save to XLSX ---

def save_to_xlsx(results_data, filename):
    """Converts the results dictionary to a Pandas DataFrame and saves it to an XLSX file."""
    try:
        # Convert dictionary of dictionaries into a DataFrame
        df = pd.DataFrame.from_dict(results_data, orient='index')
        
        # Reset index to make 'Username' a column
        df = df.reset_index().rename(columns={'index': 'Username'})
        
        # Rename columns for clarity
        df.columns = ['Username', 'Push Count', 'Last Push Date/Time', 'Last Commit SHA']
        
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
            # We now store a dictionary of details keyed by username
            results_details = {} 
            total_pushes = 0
            
            print("-" * 75)
            print("Processing events and finding latest push activity for each user...")
            
            for user in user_list:
                user_id = user['id']
                username = user['username']
                
                # Fetch count and last push details
                count, last_date, last_commit = get_user_push_count(user_id, username)
                
                # Store only users who have push events
                if count > 0:
                    results_details[username] = {
                        'count': count,
                        'last_date': last_date,
                        'last_commit': last_commit
                    }
                    total_pushes += count
                    print(f"-> {username:<20}: {count} pushes, Last: {last_date} ({last_commit[:8]}...)")

            # --- Output Results to Console and XLSX ---
            
            # 1. Console Output (same as before, now sourced from results_details)
            print("\n" + "=" * 100)
            print(f"✅ GitLab User Push Event Summary & Last Activity ({MONTHS_TO_GO_BACK} Months)")
            print("=" * 100)
            
            # Sort the results for console output
            sorted_results = sorted(
                results_details.items(), 
                key=lambda item: item[1]['count'], 
                reverse=True
            )
            
            # Print Header
            print(f"{'Username':<25} {'Pushes':<8} {'Last Push Date':<20} {'Last Commit SHA (First 8)':<20}")
            print("-" * 100)

            for username, data in sorted_results:
                print(f"{username:<25} {data['count']:<8} {data['last_date']:<20} {data['last_commit'][:8]:<20}")
                
            print("-" * 100)
            print(f"Total Unique Contributors: {len(results_details)}")
            print(f"Total Push Events: {total_pushes}")
            print("=" * 100)
            
            # 2. XLSX Output
            if results_details:
                save_to_xlsx(results_details, OUTPUT_FILENAME)