import os
import pandas as pd

path = "./raw_scraped_data/"
print(os.listdir(path))

save_path = "./bundled_data/"

reviews_folder_name = "detailed_reviews"
overview_folder_name = "overview"


def get_files_list_within_folder(folder_name):
    current_folder_path = path + folder_name + "/"
    file_list = [
        current_folder_path + file_name for file_name in os.listdir(current_folder_path)
    ]

    return file_list


def merge_csv_files_folder(folder_name):
    current_files_list = get_files_list_within_folder(folder_name)

    csv_list = []

    for file in sorted(current_files_list):
        csv_list.append(pd.read_csv(file))

    print("currently merging " + folder_name)
    csv_merged = pd.concat(csv_list, ignore_index=True)

    print("currently saving " + folder_name)

    csv_merged.to_csv(save_path + folder_name + ".csv", index=False)


merge_csv_files_folder(overview_folder_name)
merge_csv_files_folder(reviews_folder_name)
