import os
import re
import csv
from urllib.parse import unquote

def build_title_to_id_mapping_and_save(data_folder, output_filename='title_to_id_mapping.csv'):
    """
    Builds a dictionary mapping cleaned article titles to their article IDs by:
    1. Iterating through all CSV files in the specified folder.
    2. Extracting the article title from the filename.
    3. Reading the article_id from the 'article_id' column inside the CSV.
    
    Then, it saves the resulting mapping to a two-column CSV file.

    Args:
        data_folder (str): The path to the folder containing the data files.
        output_filename (str): The name of the output CSV file for the mapping.
    """
    title_to_id = {}
    
    if not os.path.isdir(data_folder):
        print(f"Error: The folder '{data_folder}' does not exist.")
        return

    print("--- Phase 1: Building the Title-to-ID Mapping ---")
    
    for filename in os.listdir(data_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(data_folder, filename)
            
            try:
                # Extract and clean the title from the filename
                raw_title = unquote(filename.rsplit('.', 1)[0])
                cleaned_title = re.sub(r'[^a-z0-9_]', '', raw_title.lower().replace(' ', '_'))

                with open(file_path, mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    first_row = next(reader, None)

                    if first_row and 'article_id' in first_row:
                        article_id = int(first_row['article_id'])
                        title_to_id[cleaned_title] = article_id
                        # Optional: Print progress for large datasets
                        # print(f"Mapped '{cleaned_title}' to ID {article_id}")
            
            except Exception as e:
                print(f"Error processing file '{filename}': {e}")
                
    if not title_to_id:
        print("\nNo articles were processed. Please check the folder path and file contents.")
        return

    print(f"\nSuccessfully created a mapping for {len(title_to_id)} articles.")
    print("Example mappings:")
    for i, (title, article_id) in enumerate(title_to_id.items()):
        if i < 5:
            print(f"  {title}: {article_id}")
        else:
            break

    print(f"\n--- Phase 2: Saving the Mapping to '{output_filename}' ---")
    
    try:
        with open(output_filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['cleaned_title', 'article_id'])
            for title, article_id in title_to_id.items():
                writer.writerow([title, article_id])
        
        print(f"Successfully saved the mapping to '{output_filename}'.")
    except Exception as e:
        print(f"Error saving the file: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    # Define the folder containing your data files
    # Make sure this path is correct for your system
    data_directory = '/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html'  

    # Run the combined function
    build_title_to_id_mapping_and_save(data_directory)