
import os

class Utils():
    def file_exists(folder_path, file_name):
    
        full_path = os.path.join(folder_path, file_name)
        return os.path.isfile(full_path)

    def age_to_bucket(age: int):
        if age < 10:
            age_bucket = "0-9"
        elif age >= 100:
            age_bucket = "100+"
        else:
            parcial = int(str(age)[0])
            age_bucket = f"{parcial}0-{parcial}9"
        
        return age_bucket
    
    def categorize_address(address, category):
        water_keywords = ['lake', 'creek', 'river', 'spring', 'ocean', 'seashore', 'beach', 'coastal', 'waterfront']
        relief_keywords = ['hill', 'mountain', 'canyon', 'valley', 'cliff']
        flat_keywords = ['plain', 'plateau', 'field']

        address_lower = address.lower()
        
        if category == "water":
            return any(keyword in address_lower for keyword in water_keywords)
        elif category == 'relief':
            return any(keyword in address_lower for keyword in relief_keywords)
        elif category == 'flat':
            return any(keyword in address_lower for keyword in flat_keywords)
    
    