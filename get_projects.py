import requests
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
# Read the environment variables
GITLAB_URL = os.getenv("GITLAB_URL")
PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")

# API endpoint for listing projects.
# The `per_page` parameter is set high to minimize API calls, 
# and the `owned=true` parameter filters for projects owned by the user associated with the token.
PROJECTS_API_URL = f"{GITLAB_URL}/api/v4/projects?owned=true&per_page=100"

# Header with the private token for authentication
HEADERS = {
    "Private-Token": PRIVATE_TOKEN
}

def get_all_project_ids():
    """
    Retrieves all project IDs from GitLab using the API, handling pagination.
    """
    all_project_ids = []
    page = 1
    
    # Loop to handle pagination (requesting pages until no more projects are returned)
    while True:
        # Build the URL for the current page
        current_url = f"{PROJECTS_API_URL}&page={page}"
        print(f"-> Fetching page {page} from: {current_url}")
        
        try:
            response = requests.get(current_url, headers=HEADERS)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            
            projects = response.json()
            
            if not projects:
                # No more projects found, break the loop
                break
                
            # Extract the ID and path_with_namespace (full name) for each project
            for project in projects:
                project_id = project.get('id')
                project_name = project.get('path_with_namespace')
                if project_id:
                    all_project_ids.append((project_id, project_name))
            
            # Move to the next page
            page += 1

        except requests.exceptions.RequestException as e:
            print(f"An error occurred during API request: {e}")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    return all_project_ids

if __name__ == "__main__":
    if not GITLAB_URL or not PRIVATE_TOKEN:
        print("Error: GITLAB_URL or GITLAB_PRIVATE_TOKEN not found in .env file.")
    else:
        print("Starting GitLab project retrieval...")
        project_data = get_all_project_ids()

        if project_data:
            print("\n✅ Successfully retrieved the following projects:")
            print("-" * 40)
            for project_id, project_name in project_data:
                print(f"ID: {project_id:<10} | Name: {project_name}")
            print("-" * 40)
            print(f"Total projects found: {len(project_data)}")
        else:
            print("\n❌ Could not retrieve any projects or an error occurred. Check your token and URL.")