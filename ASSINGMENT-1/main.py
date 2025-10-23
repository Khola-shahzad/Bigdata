import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

HDFS_HOST = os.getenv("HDFS_HOST", "namenode")
HDFS_PORT = os.getenv("HDFS_PORT", "9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
FILE_PATH = os.getenv("HDFS_FILE_PATH", "/tmp/items.txt")

HDFS_URL = f"http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1"


def create_item(item):
    print(f"Creating item: {item}")
    items = read_items(return_data=True)
    items.append(item)
    data = "\n".join(items)

    params = {"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER}
    r = requests.put(f"{HDFS_URL}{FILE_PATH}", params=params, allow_redirects=False)
    if "Location" in r.headers:
        redirect_url = r.headers["Location"]
        requests.put(redirect_url, data=data.encode())
        print("Item created successfully!")
    else:
        print("Create failed:", r.text)


def read_items(return_data=False):
    params = {"op": "OPEN", "user.name": HDFS_USER}
    try:
        r = requests.get(f"{HDFS_URL}{FILE_PATH}", params=params)
        if r.status_code == 200:
            data = r.text.strip()
            items = data.split("\n") if data else []
            if return_data:
                return items
            if not items:
                print("No items found.")
            else:
                print("All items:")
                for i, item in enumerate(items, 1):
                    print(f"{i}. {item}")
            return items
        else:
            if not return_data:
                print("No file found (empty list).")
            return []
    except Exception as e:
        print(" Read failed:", e)
        return []


def update_item(index, new_value):
    items = read_items(return_data=True)
    if 0 <= index < len(items):
        items[index] = new_value
        data = "\n".join(items)
        params = {"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER}
        r = requests.put(f"{HDFS_URL}{FILE_PATH}", params=params, allow_redirects=False)
        if "Location" in r.headers:
            redirect_url = r.headers["Location"]
            requests.put(redirect_url, data=data.encode())
            print("Item updated successfully!")
        else:
            print("Update failed:", r.text)
    else:
        print("Invalid index.")


def delete_item(index):
    items = read_items(return_data=True)
    if 0 <= index < len(items):
        deleted = items.pop(index)
        data = "\n".join(items)
        params = {"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER}
        r = requests.put(f"{HDFS_URL}{FILE_PATH}", params=params, allow_redirects=False)
        if "Location" in r.headers:
            redirect_url = r.headers["Location"]
            requests.put(redirect_url, data=data.encode())
            print(f"Deleted item: {deleted}")
        else:
            print("Delete failed:", r.text)
    else:
        print("Invalid index.")


def main():
    while True:
        print("\n=== HDFS CRUD MENU ===")
        print("1. Create Item")
        print("2. Read Items")
        print("3. Update Item")
        print("4. Delete Item")
        print("5. Exit")

        choice = input("Enter choice: ").strip()

        if choice == "1":
            item = input("Enter item to add: ")
            create_item(item)
        elif choice == "2":
            read_items()
        elif choice == "3":
            read_items()
            index = int(input("Enter item number to update: ")) - 1
            new_value = input("Enter new value: ")
            update_item(index, new_value)
        elif choice == "4":
            read_items()
            index = int(input("Enter item number to delete: ")) - 1
            delete_item(index)
        elif choice == "5":
            print("Exiting...")
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    main()
