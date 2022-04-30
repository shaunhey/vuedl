#define _GNU_SOURCE

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <json-c/json.h>
#include <sqlite3.h>

int json_filter(const struct dirent *d)
{
    const char *ext = strstr(d->d_name, ".json");
    return (ext && strlen(ext) == 5);
}

int get_device_name(const char *filename, char *device_name)
{
    const char *start, *sep, *end;
    if ((start = strchr(filename, '_')) &&
        (sep = strchr(start+1, '_')) &&
        (end = strchr(sep+1, '_')))
    {
        int len = end - start - 1;
        memcpy(device_name, start+1, len);
        device_name[len] = 0;
        return EXIT_SUCCESS;
    }
    return EXIT_FAILURE;
}

void insert_reading(sqlite3 *db, const char *timestamp, const char *device_name, double value)
{
    //printf("%s %s %0.17f\n", device_name, timestamp, value);
    char *sql = "insert into readings (timestamp, device, value) values (?, ?, ?)";

    int rc;
    sqlite3_stmt *stmt;
    if ((rc = sqlite3_prepare(db, sql, -1, &stmt, NULL)) ||
        (rc = sqlite3_bind_text(stmt, 1, timestamp, -1, SQLITE_STATIC)) ||
        (rc = sqlite3_bind_text(stmt, 2, device_name, -1, SQLITE_STATIC)) ||
        (rc = sqlite3_bind_double(stmt, 3, value)))
    {
        fprintf(stderr, "Failed to prepare insert statement (rc = %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    if ((rc = sqlite3_step(stmt) != SQLITE_DONE))
    {
        fprintf(stderr, "Failed to insert reading (rc = %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    sqlite3_finalize(stmt);
}

void increment_timestamp(char *timestamp)
{
    struct tm tm;
    strptime(timestamp, "%FT%T%z", &tm);
    tm.tm_isdst = 0;
    tm.tm_min++;
    mktime(&tm);
    strftime(timestamp, 32, "%FT%TZ", &tm);
}

void ingest_file(const char *filename, sqlite3 *db)
{
    fprintf(stderr, "Processing %s...\n", filename);

    char device_name[64];
    if (get_device_name(filename, device_name))
    {
        fprintf(stderr, "Error parsing filename %s\n", filename);
        exit(EXIT_FAILURE);
    }

    struct json_object *object = json_object_from_file(filename);
    if (!object)
    {
        fprintf(stderr, "Failed to parse file %s\n", filename);
        exit(EXIT_FAILURE);
    }

    json_object *first_usage_instant = json_object_object_get(object, "firstUsageInstant");
    if (first_usage_instant)
    {
        char timestamp[32];

        strcpy(timestamp, json_object_get_string(first_usage_instant));

        json_object *usage_list = json_object_object_get(object, "usageList");
        if (usage_list)
        {
            int count = json_object_array_length(usage_list);
            for (int i = 0; i < count; i++)
            {
                json_object *usage_object = json_object_array_get_idx(usage_list, i);
                if (usage_object)
                {
                    double value = json_object_get_double(usage_object);
                    insert_reading(db, timestamp, device_name, value);
                }
                increment_timestamp(timestamp);
            }
        }
    }
    json_object_put(object);
}

int update_db(sqlite3 *db)
{
    char *errmsg = NULL;
    const char *sql =
        "CREATE TABLE IF NOT EXISTS readings ("
	    "timestamp NUMERIC NOT NULL,"
	    "device TEXT NOT NULL,"
	    "value REAL NOT NULL,"
	    "PRIMARY KEY(device, timestamp)"
	    ");";

    int rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
    if (rc)
    {
        fprintf(stderr, "Error creating database: %s\n", errmsg);
        exit(EXIT_FAILURE);
    }

    sql = "PRAGMA journal_mode=WAL;";
    rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
    if (rc)
    {
        fprintf(stderr, "Error creating database: %s\n", errmsg);
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}

sqlite3 *get_db(const char *filename)
{
    sqlite3 *db;
    int rc = sqlite3_open(filename, &db);
    if (rc)
    {
        fprintf(stderr, "Error opening database\n");
        exit(EXIT_FAILURE);
    }
    update_db(db);
    return db;
}

int main(void)
{
    sqlite3 *db = get_db("./vue.db");

    const char *dir = "/var/lib/vuedl/";
    struct dirent **files;
    int count = scandir(dir, &files, json_filter, alphasort);
    if (count == -1)
    {
       perror(NULL);
       exit(EXIT_FAILURE);
    }

    sqlite3_exec(db, "begin transaction;", NULL, NULL, NULL);


    for (int i = 0; i < count; i++)
    {
        char *filename;
        asprintf(&filename, "%s%s", dir, files[i]->d_name);
        ingest_file(filename, db);
        free(filename);
        free(files[i]);
    }


    sqlite3_exec(db, "commit transaction;", NULL, NULL, NULL);

    free(files);
    sqlite3_close(db);
    return EXIT_SUCCESS;
}

