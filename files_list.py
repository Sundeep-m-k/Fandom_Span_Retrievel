import os

# set your folder path here
folder_path = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_data/alldimensions_fandom_html"
output_file = "file_list.txt"

with open(output_file, "w", encoding="utf-8") as f:
    for filename in os.listdir(folder_path):
        if os.path.isfile(os.path.join(folder_path, filename)):
            f.write(filename + "\n")

print(f"File names saved to {output_file}")