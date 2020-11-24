import os
import pandas as pd
import json
from jsonschema import Draft7Validator, validate, exceptions


def open_json_file(json_file):
    with open(json_file, 'r') as f:
        json_data = f.read()
    opened_json = json.loads(json_data)
    return opened_json


def validation_json(json_data, schema, file_name):
    validator = Draft7Validator(schema)

    errors = validator.iter_errors(json_data)
    errors_list = []
    recommendation = ""
    for error in errors:
        path = error.path
        if error.validator == "required":
            if not path:
                path = "Core"
            else:
                path = json_data.get(error.path[0])[error.path[1]]
            recommendation = "need to add {} to {}".format(error.message.replace("is a required property", ""), path)
        elif error.validator == "type":
            message = error.message.split("is not of type")
            recommendation = "need to change {} in {} to {}".format(message[0], error.path[0], message[1])

        errors_list.append([file_name,
                            error.message,
                            recommendation])
    return errors_list


def get_schema_df(schema_path):
    schema_list = []
    schema_name_list = []
    for json_schema in os.listdir(schema_path):
        full_path_to_schema = "/".join([schema_path, json_schema])
        schema = open_json_file(full_path_to_schema)
        schema_list.append(schema)
        schema_name_list.append(json_schema.replace(".schema", ""))
    schema_data = {"schema_name": schema_name_list,
                   "schema_json": schema_list}
    return pd.DataFrame(schema_data)


def get_json_df(path):
    json_events_list = []
    json_events_names_list = []
    json_events_types_list = []
    for json_file in os.listdir(path):
        full_path_to_json = "/".join([path, json_file])
        json_event = open_json_file(full_path_to_json)
        try:
            json_events_list.append(json_event['data'])
        except (AttributeError, TypeError, KeyError):
            json_events_list.append("no data")
        json_events_names_list.append(json_file)
        try:
            json_events_types_list.append(json_event['event'].replace(" ", ""))
        except (AttributeError, TypeError, KeyError):
            json_events_types_list.append("no type")

    data = {"name": json_events_names_list,
            "text": json_events_list,
            "schema_name": json_events_types_list}
    return pd.DataFrame(data)


def created_recommendation_file(full_json_df):
    i = 0
    all_errors_list = []
    while i < len(full_json_df):
        data = full_json_df["text"][i]
        schema = full_json_df["schema_json"][i]
        file_name = full_json_df["name"][i]
        if data == "no_data" or not data:
            all_errors_list.append([file_name,
                                    "no data",
                                    "need to add data in file"])
        else:
            try:
                all_errors_list.extend(validation_json(data, schema, file_name))
            except (TypeError, AttributeError):
                all_errors_list.append([file_name,
                                        "no schema",
                                        "need to add {} json schema".format(full_json_df['schema_name'][i])])
        i += 1

    file_and_problem_df = pd.DataFrame(data=all_errors_list,
                                       columns=["file_name",
                                                "problem",
                                                "recommendation"])
    html = file_and_problem_df.to_html()
    with open("README.md", "w") as file:
        file.write(html)
