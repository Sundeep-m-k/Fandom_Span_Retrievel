import pandas as pd

# Load the dataframes from your CSV files
try:
    paragraphs_df = pd.read_csv('/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv')
    links_df = pd.read_csv('/home/sundeep/Fandom-Span-Identification-and-Retrieval/processed_links_by_paragraph.csv')
    titles_df = pd.read_csv('/home/sundeep/Fandom-Span-Identification-and-Retrieval/title_to_id_mapping.csv')

    # Merge paragraphs_df and links_df on 'article_id' and 'paragraph_id'
    # The 'how=inner' ensures that only rows with matching 'article_id' and 'paragraph_id' in both dataframes are kept.
    merged_df = pd.merge(paragraphs_df, links_df, on=['article_id', 'paragraph_id'], how='inner')

    # Merge the result with titles_df on 'article_id'
    # The 'how=left' ensures all rows from the previous merge are kept, and matching 'cleaned_title' values are added.
    master_df = pd.merge(merged_df, titles_df, on='article_id', how='left')

    # Save the final dataframe to a new CSV file called 'master_csv.csv'
    # 'index=False' prevents pandas from writing the dataframe's index as a column in the CSV.
    master_df.to_csv('master_csv.csv', index=False)

    print("Successfully created 'master_csv.csv' with the combined data.")
    print("The final master_csv contains the following columns:")
    print(master_df.columns.tolist())

except FileNotFoundError as e:
    print(f"Error: One of the input files was not found. Please ensure that 'paragraphs.csv', 'processesd_links_by_paragraphs.csv', and 'title_to_id_mapping.csv' are in the same directory.")
    print(e)
except Exception as e:
    print(f"An unexpected error occurred: {e}")