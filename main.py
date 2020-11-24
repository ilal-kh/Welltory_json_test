from validat_json_by_schema import get_schema_df, get_json_df, created_recommendation_file


schema_json_df = get_schema_df("task_folder/schema")
json_df = get_json_df("task_folder/event")
full_json_df = json_df.merge(schema_json_df,
                             on="schema_name",
                             how="left")
created_recommendation_file(full_json_df)