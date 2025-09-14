import os
import re
import pandas as pd
import csv
from urllib.parse import unquote

# Helper function to load the mapping from the CSV file
def load_mapping_from_csv(input_filename):
    """Loads a two-column CSV file into a dictionary."""
    loaded_map = {}
    try:
        with open(input_filename, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                title = row['cleaned_title']
                article_id = int(row['article_id'])
                loaded_map[title] = article_id
        return loaded_map
    except FileNotFoundError:
        print(f"Error: The mapping file '{input_filename}' was not found.")
        return {}
    except Exception as e:
        print(f"Error loading the mapping file: {e}")
        return {}

# Helper function to clean link text
def clean_link_text(text):
    """Cleans a string to match the title format in the mapping dictionary."""
    if pd.isna(text):
        return None
    cleaned_text = re.sub(r'[^a-z0-9_]', '', str(text).lower().replace(' ', '_'))
    return unquote(cleaned_text)

def process_links_and_group_by_paragraph(data_folder, mapping_file):
    """
    Processes all CSV files in a folder, resolves internal links,
    and groups the results by paragraph.
    
    Args:
        data_folder (str): Path to the folder with article CSVs.
        mapping_file (str): Path to the title_to_id mapping CSV.
        
    Returns:
        pd.DataFrame: A DataFrame with one row per paragraph, containing a list
                      of resolved links and their original text.
    """
    # 1. Load the title_to_id mapping
    print("--- Phase 2: Processing CSV Data ---")
    print(f"Loading title-to-ID mapping from '{mapping_file}'...")
    title_to_id_map = load_mapping_from_csv(mapping_file)
    if not title_to_id_map:
        return pd.DataFrame() # Return an empty DataFrame if mapping failed to load

    # 2. Load all CSV files into a single DataFrame
    all_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if f.endswith('.csv')]
    if not all_files:
        print(f"No CSV files found in '{data_folder}'.")
        return pd.DataFrame()
        
    print(f"Found {len(all_files)} CSV files. Combining them into a single DataFrame...")
    try:
        # Use a list comprehension to read all files and then concatenate
        df = pd.concat([pd.read_csv(f) for f in all_files], ignore_index=True)
    except Exception as e:
        print(f"Error combining CSV files: {e}")
        return pd.DataFrame()

    # 3. Create a lookup function that works on a pandas Series
    def resolve_links(link_texts):
        resolved_ids = []
        original_texts = []
        for text in link_texts:
            cleaned = clean_link_text(text)
            if cleaned and cleaned in title_to_id_map:
                resolved_ids.append(title_to_id_map[cleaned])
                original_texts.append(text)
        return pd.Series([original_texts, resolved_ids], index=['internal_links', 'article_id_of_internal_link'])

    # 4. Group by paragraph and apply the lookup function
    print("Grouping data by paragraph and resolving links...")
    processed_df = df.groupby(['article_id', 'paragraph_id'])['link_text'].apply(
        resolve_links
    ).unstack().reset_index()

    print("\nProcessing complete. Final DataFrame created.")
    return processed_df

# --- Main Execution Block ---
if __name__ == "__main__":
    # Define your folder and file paths
    data_directory = '/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html' # The folder with your CSV files
    mapping_filename = '/home/sundeep/Fandom-Span-Identification-and-Retrieval/title_to_id_mapping.csv' # The output from Phase 1

    # Run the main processing function
    result_df = process_links_and_group_by_paragraph(data_directory, mapping_filename)

    if not result_df.empty:
        # Display the first few rows of the result DataFrame
        print("\n--- Resulting DataFrame (First 5 rows) ---")
        print(result_df.head())
        print(f"\nDataFrame shape: {result_df.shape}")
        
        # Optional: Save the result DataFrame to a new CSV file for the next phase
        result_df.to_csv('processed_links_by_paragraph.csv', index=False)
        print("\nResults saved to 'processed_links_by_paragraph.csv'.")