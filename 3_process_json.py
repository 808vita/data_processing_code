import json

json_data = {}
with open(
    "./processed_data/identifyProductsPromptprocessedMonthSplitReviewsDict.json"
) as f:
    json_data = json.load(f)
    print(json_data)

branch_month_for_json = {}

for key in json_data.keys():
    available_months_list = []
    branch_data = json_data[key]
    for month in branch_data.keys():
        month_data = branch_data[month]
        if len(month_data) > 0:
            available_months_list.append(month)

    branch_month_for_json[key] = available_months_list


with open("new_processed_branch_month_with_products.json", "w") as f:
    json.dump(branch_month_for_json, f)
