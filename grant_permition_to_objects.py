import os
import snowflake.connector
import json
import logging
import boto3


def get_snowflake_info(ssm_client):
    session = boto3.session.Session()
    ssm_client = session.client('ssm', region_name='us-east-1')
    env_param = ssm_client.get_parameter(Name='env', WithDecryption=True)
    env = env_param.get('Parameter').get('Value')
    logging.info('Getting env info')
    env_value = env

    snf_acc_value = ssm_client.get_parameter(
        Name='/users/snowflake/account',
        WithDecryption=True).get('Parameter').get('Value')

    snf_user_value = ssm_client.get_parameter(
        Name='/users/snowflake/account/user',
        WithDecryption=True).get(
        'Parameter').get('Value')

    snf_key_value = ssm_client.get_parameter(
        Name='/users/snowflake/account/password',
        WithDecryption=True).get(
        'Parameter').get('Value')

    env_var = {'env': env_value, 'snf_account': snf_acc_value,
               'snf_user': snf_user_value, 'snf_key': snf_key_value}

    snf_role = 'SYSADMIN'
    snf_schema = "SCHEMA1"
    snf_wh = 'SNF_POC_XS_WH'
    env_var['warehouse'] = snf_wh
    env_var['schema'] = snf_schema
    env_var['role'] = snf_role

    if (env_value == 'qa'):
        env_var['database'] = ''
    elif (env_value == 'prod'):
        env_var['database'] = ''
    else:
        env_var['database'] = 'DEV_CZ'
    return env_var


def get_recently_created_objects(connection, object_type, config):
    try:
        database = None

        if object_type != 'DATABASE':
            for item in config:
                database = item.get("database")
                if database:
                    break

            if not database:
                print("Database not found in config")
                return

            if object_type.upper() == 'TABLE':
                query = f"""
                    SELECT TABLE_NAME
                    FROM {database}.INFORMATION_SCHEMA.TABLES
                    WHERE CREATED >= DATEADD(MINUTES, -5, CURRENT_TIMESTAMP()) and TABLE_TYPE = 'BASE TABLE'
                """
            elif object_type.upper() == 'VIEW':
                query = f"""
                    SELECT TABLE_NAME
                    FROM {database}.INFORMATION_SCHEMA.VIEWS
                    WHERE CREATED >= DATEADD(MINUTES, -5, CURRENT_TIMESTAMP())
                """
            elif object_type.upper() == 'SCHEMA':
                query = f"""
                    SELECT SCHEMA_NAME
                    FROM {database}.INFORMATION_SCHEMA.SCHEMATA
                    WHERE CREATED >= DATEADD(MINUTES, -5, CURRENT_TIMESTAMP())
                """
            else:
                print(f"Object Type Is Not Valid: {object_type}")
                return
        else:
            query = """
            SELECT DATABASE_NAME
            FROM INFORMATION_SCHEMA.DATABASES
            WHERE CREATED >= DATEADD(MINUTES, -5, CURRENT_TIMESTAMP());
            """
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error occurred while fetching recently created {object_type}s:", e)
        return []


# Function to compare created tables/views with configuration
def compare_objects_with_config(config, created_objects, object_type):
    try:
        if not config:
            print(f"No configuration available for {object_type}s")
            return

        matched = []
        unmatched = []

        for obj in created_objects:
            obj_name = obj[0].upper()

            for item in config:
                obj_name_config = item.get("object_name", "").upper()
                obj_type = item.get("object_type", "").upper()

                if obj_type == object_type.upper():
                    if object_type == 'TABLE':
                        if obj_name_config == obj_name:
                            matched.append(obj_name)
                            break
                    if object_type == 'VIEW':
                        if obj_name_config == obj_name:
                            matched.append(obj_name)
                            break
                    elif object_type == 'SCHEMA':
                        schema_config = item.get("schema", "").upper()
                        if schema_config == obj_name:
                            matched.append(obj_name)
                            break
                    elif object_type == 'DATABASE':
                        database_config = item.get("database", "").upper()
                        if database_config == obj_name:
                            matched.append(obj_name)
                            break
            else:
                unmatched.append(obj_name)

        return matched, unmatched
    except Exception as e:
        print(f"Error occurred while comparing {object_type}s with config:", e)
        return [], []


def grantee_privilege_exists(database, schema, object_name, grantee, privilege_type, object_type, connection):
    try:
        database = database.upper()
        schema = schema.upper()
        object_name = object_name.upper()
        grantee = grantee.upper()
        privilege_type = privilege_type.upper()
        object_type = object_type.upper()
        

        if object_type == "TABLE" or object_type == "VIEW":
            query = f"""
                SELECT PRIVILEGE_TYPE
                FROM {database}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
                WHERE OBJECT_SCHEMA = '{schema}'
                AND OBJECT_CATALOG = '{database}'
                AND OBJECT_NAME = '{object_name}'
                AND GRANTEE = '{grantee}'
                AND PRIVILEGE_TYPE = '{privilege_type}'
            """
        elif object_type == "SCHEMA":
            query = f"""
                SELECT PRIVILEGE_TYPE
                FROM {database}.INFORMATION_SCHEMA.OBJECT_PRIVILEGES
                WHERE object_schema = '{schema}'
                AND GRANTEE = '{grantee}'
                AND PRIVILEGE_TYPE = '{privilege_type}'
            """
            print(query)
        elif object_type == "DATABASE":
            query = f"""
                show grants on database {database}
            """

        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return bool(result)
    except Exception as e:
        print(f"Error occurred while checking privilege for {object_type}:", e)
        return False


def matched_objects_permission(config, json_data, conn):

    try:
        table_matched = json_data["tables"]["matched_tables"]
        view_matched = json_data["views"]["matched_views"]
        schema_matched = json_data["schema"]["matched_schema"]
        matched_database = json_data["database"]["matched_database"]
        for item in config:
            database = item.get("database", "").upper()
            schema = item.get("schema", "").upper()
            object_name_config = item.get("object_name", "").upper()
            object_type = item.get("object_type", "").upper()
            grants = item.get("GRANTEE", [])
            enforcement_action = item.get("enforcement_action")
            table_matched_objects = []
            view_matched_objects = []
            if object_type == "TABLE":
                table_matched_objects = table_matched
            elif object_type == "VIEW":
                view_matched_objects = view_matched
            elif object_type == "SCHEMA":
                schema_matched_objects = schema_matched
            elif object_type == "DATABASE":
                database_matched_objects = matched_database
            else:
                continue
            if object_type == "TABLE":
                for matched_object in table_matched_objects:
                    if object_name_config == matched_object:
                        print(f"    Assigning privileges for {object_type} '{matched_object}' based on config...")
                        for grant in grants:
                            grantee = list(grant.keys())[0]
                            privileges = grant[grantee]
                            for privilege_type in privileges:
                                privilege_type = privilege_type
                                if not grantee_privilege_exists(database, schema, matched_object, grantee, privilege_type, object_type, conn):
                                    if enforcement_action == "enforce":
                                        conn.cursor().execute("alter session set query_tag='matched_enforce_table';")
                                        revoke_query = f"REVOKE ALL PRIVILEGES ON {object_type} {database}.{schema}.{matched_object} FROM ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(revoke_query)
                                        print(f"        Revoked all privileges on {object_type} {database}.{schema}.{matched_object} from role {grantee}")
                                        grant_query = f"GRANT {privilege_type} ON {object_type} {database}.{schema}.{matched_object} TO ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} on {object_type} {database}.{schema}.{matched_object} to role {grantee}")
                                    elif enforcement_action == "merge":
                                        conn.cursor().execute("alter session set query_tag='matched_merge_table';")
                                        grant_query = f"GRANT {privilege_type} ON TABLE {database}.{schema}.{matched_object} TO ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} on TABLE {database}.{schema}.{matched_object} to role {grantee}")
                                else:
                                    print(f"        Privilege already exists for {database}.{schema}.{matched_object}: Object: {database}.{schema}.{matched_object}, grantee: {grantee}, Privilege: {privilege_type}")
                        print("")
            elif object_type == "VIEW":
                for matched_object in view_matched_objects:
                    if object_name_config == matched_object:
                        print(f"    Assigning privileges for {object_type} '{matched_object}' based on config...")
                        for grant in grants:
                            grantee = list(grant.keys())[0]
                            privileges = grant[grantee]

                            privileges = grant[grantee]
                            for privilege_type in privileges:
                                privilege_type = privilege_type
                                if not grantee_privilege_exists(database, schema, matched_object, grantee, privilege_type, object_type, conn):
                                    if enforcement_action == "enforce":
                                        conn.cursor().execute("alter session set query_tag='matched_enforce_view';")
                                        revoke_query = f"REVOKE ALL PRIVILEGES ON {object_type} {database}.{schema}.{matched_object} FROM ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(revoke_query)
                                        print(f"        Revoked all privileges on {object_type} {database}.{schema}.{matched_object} from role {grantee}")
                                        grant_query = f"GRANT {privilege_type} ON {object_type} {database}.{schema}.{matched_object} TO ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} ON {object_type} {database}.{schema}.{matched_object} to role {grantee}")
                                    elif enforcement_action == "merge":
                                        conn.cursor().execute("alter session set query_tag='matched_merge_view';")
                                        grant_query = f"GRANT {privilege_type} ON {object_type} {database}.{schema}.{matched_object} TO ROLE {grantee}"

                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} on {object_type} {database}.{schema}.{matched_object} to role {grantee}")
                                else:
                                    print(f"        Privilege already exists for {database}.{schema}.{matched_object}: Object: {database}.{schema}.{matched_object}, grantee: {grantee}, Privilege: {privilege_type}")
                        print("")
            elif object_type == "SCHEMA":
                for schema_matched_object in schema_matched_objects:
                    if schema_matched_object == schema:
                        print(f"    Assigning privileges for {object_type} '{schema_matched_object}' based on config...")
                        for grant in grants:
                            grantee = list(grant.keys())[0]
                            privileges = grant[grantee]
                            for privilege_type in privileges:
                                privilege_type = privilege_type
                                if not grantee_privilege_exists(database, schema, schema_matched_object, grantee, privilege_type, object_type, conn):
                                    if enforcement_action == "enforce":
                                        conn.cursor().execute("alter session set query_tag='matched_enforce_schema';")
                                        revoke_query = f"REVOKE {privilege_type} ON SCHEMA {database}.{schema} FROM ROLE {grantee};"
                                        cursor = conn.cursor()
                                        cursor.execute(revoke_query)
                                        print(f"        Revoked {privilege_type} ON Role: {grantee} and SCHEMA: {database}.{schema}")
                                        grant_query = f"GRANT {privilege_type} ON SCHEMA {database}.{schema} TO ROLE {grantee};"

                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} ON Role :  {grantee} and SCHEMA is  {database}.{schema} ")
                                    elif enforcement_action == "merge":
                                        conn.cursor().execute("alter session set query_tag='matched_merge_schema';")
                                        grant_query = f"GRANT {privilege_type} ON SCHEMA {database}.{schema} TO ROLE {grantee};"
                                        print("        ", grant_query)
                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"        Granted {privilege_type} on {object_type} {database}.{schema_matched_object} to role {grantee}")
                                else:
                                    print(f"        Privilege already exists for {database}.{schema}: Object: {database}.{schema} , grantee: {grantee}, Privilege: {privilege_type}")
                        print("")
            elif object_type == "DATABASE":
                for database_matched_object in database_matched_objects:
                    if database_matched_object == database:
                        print(f"    Assigning privileges for {object_type} '{database_matched_object}' based on config...")
                        for grant in grants:
                            grantee = list(grant.keys())[0]
                            # grantee = list(grant.keys())[0].upper()

                            privileges = grant[grantee]
                            for privilege_type in privileges:
                                privilege_type = privilege_type.upper()
                                # Ensure privilege_type is uppercase
                                privilege_type = privilege_type.upper()
                                # Check if the privilege already exists before granting
                                if not grantee_privilege_exists(database, '', database, grantee, privilege_type, object_type, conn):
                                    if enforcement_action == "enforce":
                                        conn.cursor().execute("alter session set query_tag='matched_enforce_database';")
                                        revoke_query = f"REVOKE {privilege_type} ON DATABASE {database} FROM ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(revoke_query)
                                        print(f"    Revoked {privilege_type} on {object_type} {database} from role {grantee}")
                                        grant_query = f"GRANT {privilege_type} ON DATABASE {database} TO ROLE {grantee}"
                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"    Granted {privilege_type} on {object_type} {database} to role {grantee}")
                                    elif enforcement_action == "merge":
                                        conn.cursor().execute("alter session set query_tag='matched_merge_database';")
                                        # Adjust the grant_query to grant the specified privilege type
                                        grant_query = f"GRANT {privilege_type} ON DATABASE {database} TO ROLE {grantee}"
                                        print("Grant Query is:", grant_query)

                                        cursor = conn.cursor()
                                        cursor.execute(grant_query)
                                        print(f"    Granted {privilege_type} on {object_type} {database} to role {grantee}")
                                else:
                                    print(f"    Privilege already exists for {database}: Object: {database}, grantee: {grantee}, Privilege: {privilege_type}")
                        print("")

    except Exception as e:
        print("Error occurred while processing matched objects:", e)


def unmatched_objects_permission(config, json_data, conn):

    try:
        unmatched_tables = json_data["tables"]["unmatched_tables"]
        unmatched_views = json_data["views"]["unmatched_views"]
        unmatched_schema = json_data["schema"]["unmatched_schema"]
        unmatched_database = json_data["database"]["unmatched_database"]
        if not any(item['object_type'] == 'TABLE' and not item.get('object_name') for item in config):
            print(" ")
        else:
            print("    Table: ")
            i = 0
            for table in unmatched_tables:
                i += 1
                print(f"        {i}) {table}")

        if not any(item['object_type'] == 'VIEW' and not item.get('object_name') for item in config):
            print(" ")
        else:
            print("    VIEWS:")
            j = 0
            for view in unmatched_views:
                j += 1
                print(f"        {j}) {view}")

        print(" ")

        if not any(item['object_type'] == 'SCHEMA' and not item.get('schema') for item in config):
            print(" ")
        else:
            print("    SCHEMA:")
            k = 0
            for schema in unmatched_schema:
                k += 1
                print(f"        {k}) {schema}")

        print("")

        if not any(item['object_type'] == 'DATABASE' and not item.get('database') for item in config):
            print(" ")
        else:
            print("    DATABASE:")
            x = 0
            for database in unmatched_database:
                x += 1
                print(f"        {x}) {database}")

        print("")

        for item in config:
            database = item.get("database", "").upper()
            schema = item.get("schema", "").upper()
            object_name_config = item.get("object_name", "").upper()
            object_type = item.get("object_type", "").upper()
            grants = item.get("GRANTEE", [])
            enforcement_action = item.get("enforcement_action")
            if object_type == "TABLE" and not object_name_config:
                print(f"Proccess Started for unmatched objects permission: {object_type}")
                # If object name is not specified in config for tables, grant privileges to all unmatched tables
                for table in unmatched_tables:

                    print(f"    Assigning privileges for object type is : {object_type}  and table name is : {table} based on config...")
                    for grant in grants:
                        grantee = list(grant.keys())[0].upper()
                        privileges = grant[grantee]
                        for privilege_type in privileges:
                            privilege_type = privilege_type.upper()

                            if not grantee_privilege_exists(database, schema, table, grantee, privilege_type, object_type, conn):
                                if enforcement_action == "enforce":
                                    conn.cursor().execute("alter session set query_tag='unmatched_enforce_table';")

                                    revoke_query = f"REVOKE ALL PRIVILEGES ON {object_type} {database}.{schema}.{table} FROM ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(revoke_query)
                                    print(f"  GRANT {privilege_type} ON {object_type} {database}.{schema}.{table} TO ROLE {grantee}")
                            else:
                                print(f"        Privilege already exists for {object_type} {database}.{schema}.{table}: Object: {database}.{schema}.{table}, grantee: {grantee}, Privilege: {privilege_type}")
                    print("")
            elif object_type == "VIEW" and not object_name_config:
                print(f"Proccess Started for unmatched objects permission: {object_type}")
                # If object name is not specified in config for views, grant privileges to all unmatched views
                for view in unmatched_views:
                    print(f"    Assigning privileges for object type is : {object_type}  and View name is : {view} based on config...")

                    for grant in grants:
                        grantee = list(grant.keys())[0].upper()
                        privileges = grant[grantee]
                        for privilege_type in privileges:
                            privilege_type = privilege_type.upper()
                            if not grantee_privilege_exists(database, schema, view, grantee, privilege_type, object_type, conn):
                                if enforcement_action == "enforce":
                                    conn.cursor().execute("alter session set query_tag='unmatched_enforce_view';")
                                    revoke_query = f"REVOKE ALL PRIVILEGES ON {object_type} {database}.{schema}.{view} FROM ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(revoke_query)
                                    print(f"        Revoked all privileges on {object_type} {database}.{schema}.{view} from role {grantee}")
                                    grant_query = f"GRANT {privilege_type} ON {object_type} {database}.{schema}.{view} TO ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"        Granted {privilege_type} ON {object_type} {database}.{schema}.{view} to role {grantee}")
                                elif enforcement_action == "merge":
                                    conn.cursor().execute("alter session set query_tag='unmatched_merge_view';")
                                    grant_query = f"GRANT {privilege_type} ON {object_type} {database}.{schema}.{view} TO ROLE {grantee}"
                                    print(grant_query)
                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"        Granted {privilege_type} ON {object_type} {database}.{schema}.{view} to role {grantee}")
                            else:
                                print(f"        Privilege already exists for {object_type} {database}.{schema}.{view}: Object: {database}.{schema}.{view}, grantee: {grantee}, Privilege: {privilege_type}")
                    print("")
            elif object_type == "SCHEMA" and not object_name_config:
                print(f"Proccess Started for unmatched objects permission: {object_type}")
                for schema in unmatched_schema:
                    print(f"    Assigning privileges for object type is : {object_type}  and Schema name is : {schema} based on config...")
                    for grant in grants:
                        grantee = list(grant.keys())[0].upper()
                        privileges = grant[grantee]
                        for privilege_type in privileges:
                            privilege_type = privilege_type.upper()
                            table = ""
                            if not grantee_privilege_exists(database, schema, table, grantee, privilege_type, object_type, conn):
                                if enforcement_action == "enforce":
                                    conn.cursor().execute("alter session set query_tag='unmatched_enforce_schema';")
                                    revoke_query = f"REVOKE {privilege_type} ON SCHEMA {database}.{schema} FROM ROLE {grantee};"
                                    cursor = conn.cursor()
                                    cursor.execute(revoke_query)
                                    print(f"        Revoked {privilege_type} ON Role: {grantee} and SCHEMA: {database}.{schema}")
                                    grant_query = f"GRANT {privilege_type} ON SCHEMA {database}.{schema} TO ROLE {grantee};"

                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"        Granted {privilege_type} ON Role :  {grantee} and SCHEMA is  {database}.{schema} ")
                                elif enforcement_action == "merge":
                                    conn.cursor().execute("alter session set query_tag='unmatched_merge_schema';")
                                    grant_query = f"GRANT {privilege_type} ON SCHEMA {database}.{schema} TO ROLE {grantee};"
                                    print("        ", grant_query)

                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"        Granted {privilege_type} ON Role :  {grantee} and SCHEMA is  {database}.{schema} ")
                            else:
                                print(f"        Privilege already exists for schema {database}.{schema} Object: {database}.{schema}, grantee: {grantee}, Privilege: {privilege_type}")
                    print("")
            elif object_type == "DATABASE" and not object_name_config:
                print(f"Proccess Started for unmatched objects permission: {object_type}")
                for database in unmatched_database:
                    print(f"    Assigning privileges for object type is : {object_type}  and Database is : {database} based on config...")
                    for grant in grants:
                        grantee = list(grant.keys())[0].upper()
                        privileges = grant[grantee]
                        for privilege_type in privileges:
                            privilege_type = privilege_type.upper()
                            table = ""
                            if not grantee_privilege_exists(database, "", table, grantee, privilege_type, object_type, conn):
                                if enforcement_action == "enforce":
                                    conn.cursor().execute("alter session set query_tag='unmatched_enforce_database';")
                                    revoke_query = f"REVOKE {privilege_type} ON DATABASE {database} FROM ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(revoke_query)
                                    print(f"    Revoked {privilege_type} on {object_type} {database} from role {grantee}")
                                    grant_query = f"GRANT {privilege_type} ON DATABASE {database} TO ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"    Granted {privilege_type} on {object_type} {database} to role {grantee}")
                                elif enforcement_action == "merge":
                                    conn.cursor().execute("alter session set query_tag='unmatched_merge_database';")
                                    grant_query = f"GRANT {privilege_type} ON DATABASE {database} TO ROLE {grantee}"
                                    cursor = conn.cursor()
                                    cursor.execute(grant_query)
                                    print(f"    Granted {privilege_type} on {object_type} {database} to role {grantee}")
                            else:
                                print(f"    Privilege already exists for {database}: Object: {database}, grantee: {grantee}, Privilege: {privilege_type}")
                        print("")
                    print("")
    except Exception as e:
        print("Error occurred while extracting unique object types:", e)
        return []

def extract_unique_object_types(config):
    try:
        object_types = set()
        for item in config:
            object_types.add(item['object_type'])
        return list(object_types)
    except Exception as e:
        print("Error occurred while extracting unique object types:", e)
        return []


def grant_access_main():
    conn = {}
    try:
        session = boto3.session.Session()
        client = session.client('ssm', region_name='us-east-1')
        sf_secret = get_snowflake_info(client)

        sf_secret = get_snowflake_info(client)

        conn = snowflake.connector.connect(
            user=sf_secret['snf_user'],
            password=sf_secret['snf_key'],
            account=sf_secret['snf_account'],
            warehouse=sf_secret['warehouse'],
            database=sf_secret['database'],
            schema=sf_secret['schema'],
            role=sf_secret['role']
        )


        file_list = ["default_permission.json","object_vise_permission.json"]
        current_directory = os.path.dirname(os.path.realpath(__file__))
        for file in file_list:
            file_path = os.path.join(current_directory, file)
            print(file_path)

            with open(file_path, 'r') as f:
                config = json.load(f)
            for item in config:
                if "object_type" in item:
                    item["object_type"] = item["object_type"].upper()

            if conn:
                table_matched = []
                table_unmatched = []
                view_matched = []
                view_unmatched = []
                schema_matched = []

                schema_unmatched = []
                database_matched = []
                database_unmatched = []
                unique_object_types = extract_unique_object_types(config)
                print(unique_object_types)
                for obj_type in unique_object_types:
                    if obj_type == 'TABLE':
                        recently_created_tables = get_recently_created_objects(conn, obj_type, config)
                        if not recently_created_tables:
                            print("No tables found in the last 5 minutes.")
                        else:
                            table_matched, table_unmatched = compare_objects_with_config(config, recently_created_tables, obj_type)
                    elif obj_type == 'VIEW':
                        recently_created_views = get_recently_created_objects(conn, obj_type, config)
                        if not recently_created_views:
                            print("No views found in the last 5 minutes.")
                            view_matched = []
                            view_unmatched = []
                        else:
                            view_matched, view_unmatched = compare_objects_with_config(config, recently_created_views, obj_type)
                    elif obj_type == 'SCHEMA':
                        recently_created_schema = get_recently_created_objects(conn, obj_type, config)
                        if not recently_created_schema:
                            print("No schemas found in the last 5 minutes.")
                            schema_matched = []
                            schema_unmatched = []
                        else:
                            schema_matched, schema_unmatched = compare_objects_with_config(config, recently_created_schema, obj_type)
                    elif obj_type == 'DATABASE':
                        recently_created_database = get_recently_created_objects(conn, obj_type, config)
                        if not recently_created_database:
                            print("No schemas found in the last 5 minutes.")
                            database_matched = []
                            database_unmatched = []
                        else:
                            database_matched, database_unmatched = compare_objects_with_config(config, recently_created_database, obj_type)
                    else:
                        for obj_type in unique_object_types:
                            print("Currenlty Supported Objets Are: ", obj_type)

                        print(f"Currenlty Not Support {obj_type} This Object To Assign Permition")

                print("")

                json_data = {
                    "tables": {
                        "matched_tables": table_matched,
                        "unmatched_tables": table_unmatched
                    },
                    "views": {
                        "matched_views": view_matched,
                        "unmatched_views": view_unmatched
                    },
                    "schema": {
                        "matched_schema": schema_matched,
                        "unmatched_schema": schema_unmatched
                    },
                    "database": {
                        "matched_database": database_matched,
                        "unmatched_database": database_unmatched
                    }
                }

                json_string = json.dumps(json_data, indent=4)
                print(json_string)

                if (not json_data["tables"].get("matched_tables") and not json_data["tables"].get("unmatched_tables")):
                    print("Data Is Not Found In Last 5 MINUTES For TABLE")
                else:
                    table_blocks = [block for block in config if block.get('object_type') == 'TABLE']
                    if file == "object_vise_permission.json":
                        matched_objects_permission(table_blocks, json_data, conn)
                    else:
                        print("Start Assign Default permissions to object")
                        unmatched_objects_permission(table_blocks, json_data, conn)

                if (not json_data["views"].get("matched_views") and not json_data["views"].get("unmatched_views")):
                    print("Data Is Not Found In Last 5 MINUTES For VIEWS")
                else:
                    views_blocks = [block for block in config if block.get('object_type') == 'VIEW']
                    if file == "object_vise_permission.json":
                        matched_objects_permission(views_blocks, json_data, conn)
                    else:
                        print("Start Assign Default permissions to object")
                        unmatched_objects_permission(views_blocks, json_data, conn)

                if (not json_data["schema"].get("matched_schema") and not json_data["schema"].get("unmatched_schema")):
                    print("")
                    print("Data Is Not Found In Last 5 MINUTES For SCHEMA")
                else:
                    schema_blocks = [block for block in config if block.get('object_type') == 'SCHEMA']
                    if file == "object_vise_permission.json":
                        matched_objects_permission(schema_blocks, json_data, conn)
                    else:
                        print("Start Assign Default permissions to object type")
                        unmatched_objects_permission(schema_blocks, json_data, conn)
                if (not json_data["database"].get("matched_database") and not json_data["database"].get("unmatched_database")):
                    print("")
                    print("Data Is Not Found In Last 5 MINUTES For Database")
                else:
                    database_blocks = [block for block in config if block.get('object_type') == 'DATABASE']
                    if file == "object_vise_permission.json":
                        matched_objects_permission(database_blocks, json_data, conn)
                    else:
                        print("Start Assign Default permissions to object")
                        unmatched_objects_permission(database_blocks, json_data, conn)
            else:
                print("Connection not established. Exiting...")

    except Exception as e:
        print("Error occurred in main:", e)
    finally:
        if conn:
            conn.close()
