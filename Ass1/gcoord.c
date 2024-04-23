/*
 * src/tutorial/gcoord.c
 *
 ******************************************************************************
  This file contains routines that can be bound to a Postgres backend and
  called by the backend in the process of processing queries.  The calling
  format for these routines is dictated by Postgres architecture.
******************************************************************************/

#include "postgres.h"
#include <regex.h>
#include <string.h>
#include <stdbool.h>
#include "utils/elog.h"
#include "fmgr.h"
#include <string.h>
#include <ctype.h>
#include <math.h>
#include "libpq/pqformat.h"		/* needed for send/recv functions */

PG_MODULE_MAGIC;

typedef struct Gcoord
{
	int		    length;
	char		name[FLEXIBLE_ARRAY_MEMBER];

}			Gcoord;

bool check_input(char *str)
{
	char *regex1 = "^([A-Za-z ]+)[^ ],([0-9]{0,4}([.]*)[0-9]{0,4}°[NS])(,|[ ])([0-9]{0,4}([.]*)[0-9]{0,4}°[EW])($|,|[ ])|^([A-Za-z ]+),([0-9]{0,4}([.]*)[0-9]{0,4}°[EW])(,|[ ])([0-9]{0,4}([.]*)[0-9]{0,4}°[NS])($|,|[ ])";
	regex_t regex;
    int reti;

	// compile the regex
    reti = regcomp(&regex, regex1, REG_EXTENDED | REG_NOSUB);
    if (reti != 0) {
        return 1;
    }
	// match the string
    reti = regexec(&regex, str, 0, NULL, 0);
    if (reti == 0) {
		return 0;
    } else {
		return 1;
    }
    // free the regex
    regfree(&regex);
}

static char* connocal_form(char *str)
{
	str[strcspn(str, "\n")] = '\0';
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	char *nstr;
	int te=0;

	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	int i;
    for (i = 0; locationName[i] != '\0'; i++) {
        if (locationName[i] >= 'A' && locationName[i] <= 'Z') {
            locationName[i] = locationName[i] + ('a' - 'A');
        }
    }
    
    if (numFields == 5) {
        // swap latitude and longitude if they were entered in reverse order
        if (toupper(longDir) == 'S' || toupper(longDir) == 'N') {
            float temp = latitude;
            latitude = longitude;
            longitude = temp;
            char tempDir = latDir;
            latDir = longDir;
            longDir = tempDir;
        }
        
        // make sure latitude and longitude are within valid ranges
        if (latitude > 90 || latitude < 0 || longitude > 180 || longitude < 0) {
			te=-123;
        }
        
		char* result = (char*) palloc(sizeof(char) * 100);
        strcpy(result, locationName);
		strcat(result, ",");
        char strLat[20], strLong[20];
        sprintf(strLat, "%.4f", latitude);
        sprintf(strLong, "%.4f", longitude);

        strcat(result, strLat);
        strcat(result, "°");
        strncat(result, &latDir, 1);
        strcat(result, ",");
        strcat(result, strLong);
        strcat(result, "°");
        strncat(result, &longDir, 1);
        return result;

	} else {
		te=-123;
    }
	if (te!=0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"GeoCoord", str)));
}

static char* get_loc_name(char *str)
{
	str[strcspn(str, "\n")] = '\0';
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	char *nstr;
	int te=0;

	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	int i;
    for (i = 0; locationName[i] != '\0'; i++) {
        if (locationName[i] >= 'A' && locationName[i] <= 'Z') {
            locationName[i] = locationName[i] + ('a' - 'A');
        }
    }
	char* result_loc_name = (char*) palloc(sizeof(char) * (strlen(str)+1));

	strcpy(result_loc_name,locationName);
	return result_loc_name;
}

static char* get_latit(char *str)
{
	str[strcspn(str, "\n")] = '\0';
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	char *nstr;
	int te=0;

	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	int i;
    for (i = 0; locationName[i] != '\0'; i++) {
        if (locationName[i] >= 'A' && locationName[i] <= 'Z') {
            locationName[i] = locationName[i] + ('a' - 'A');
        }
    }
    
    if (numFields == 5) {
        if (toupper(longDir) == 'S' || toupper(longDir) == 'N') {
            float temp = latitude;
            latitude = longitude;
            longitude = temp;
            char tempDir = latDir;
            latDir = longDir;
            longDir = tempDir;
        }
        
        // make sure latitude and longitude are within valid ranges
        if (latitude > 90 || latitude < 0 || longitude > 180 || longitude < 0) {
			te=-123;
        }
        
		char* result_latit = (char*) palloc(sizeof(char) * (strlen(str)+1));
        char strLat[20], strLong[20];
        sprintf(strLat, "%.4f", latitude);
        sprintf(strLong, "%.4f", longitude);
		
		strcpy(result_latit,strLat);
		strcat(result_latit,"°");
		strncat(result_latit,&latDir, 1);

        return result_latit;

	} else {
		te=-123;
    }
	if (te!=0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"GeoCoord21", str)));
}
static char* get_longi(char *str)
{
	str[strcspn(str, "\n")] = '\0';
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	char *nstr;
	int te=0;

	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	int i;
    for (i = 0; locationName[i] != '\0'; i++) {
        if (locationName[i] >= 'A' && locationName[i] <= 'Z') {
            locationName[i] = locationName[i] + ('a' - 'A');
        }
    }
    
    if (numFields == 5) {
        // swap latitude and longitude if they were entered in reverse order
        if (toupper(longDir) == 'S' || toupper(longDir) == 'N') {
            float temp = latitude;
            latitude = longitude;
            longitude = temp;
            char tempDir = latDir;
            latDir = longDir;
            longDir = tempDir;
        }
        
        // make sure latitude and longitude are within valid ranges
        if (latitude > 90 || latitude < 0 || longitude > 180 || longitude < 0) {
			te=-123;
        }
        
		char* result_longi = (char*) palloc(sizeof(char) * (strlen(str)+1));
        char strLat[20], strLong[20];
        sprintf(strLat, "%.4f", latitude);
        sprintf(strLong, "%.4f", longitude);
		
		strcpy(result_longi, strLong);
		strcat(result_longi,"°");
		strncat(result_longi,&longDir, 1);

        return result_longi;

	} else {
		te=-123;
    }
	if (te!=0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"GeoCoord", str)));
}

static char* get_latit_dir(char* str)
{
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	char* latdir1 = (char*) palloc(strlen(str)+1);
	sprintf(latdir1, "%c", latDir);
	return latdir1;
}

static char* get_longi_dir(char* str)
{
	char locationName[50];
    float latitude, longitude;
    char latDir, longDir;
	int numFields = sscanf(str, "%[^,],%f°%c,%f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	if (numFields !=5)
	{
		numFields = sscanf(str, "%[^,],%f°%c %f°%c", locationName, &latitude, &latDir, &longitude, &longDir);
	}
	char* longdir1 = (char*) palloc(strlen(str)+1);
	
	sprintf(longdir1, "%c", longDir);
	return longdir1;
}

/*****************************************************************************
 * Input/Output functions
 *****************************************************************************/

PG_FUNCTION_INFO_V1(gcoord_in);

Datum
gcoord_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int length = 0;
	Gcoord    *result;

	if (check_input(str))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"GeoCoord1", str)));


	char *new_str = connocal_form(str);
	length =strlen(new_str) + (VARHDRSZ*4) + 1;
	result = (Gcoord *) palloc(VARHDRSZ+length);
	SET_VARSIZE(result, VARHDRSZ + length);
	strcpy(result->name,new_str);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(gcoord_out);

Datum
gcoord_out(PG_FUNCTION_ARGS)
{
	Gcoord    *gcoord = (Gcoord *) PG_GETARG_POINTER(0);
	char	   *result;
	result = psprintf("%s", gcoord->name);
	PG_RETURN_CSTRING(result);
}

/*****************************************************************************
 * Binary Input/Output functions
 *
 * These are optional.
 *****************************************************************************/

PG_FUNCTION_INFO_V1(gcoord_recv);

Datum
gcoord_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	Gcoord    *result;

	const char *cname = pq_getmsgstring(buf);
	int length =strlen(cname)+1;
	result = (Gcoord *) palloc(VARHDRSZ + length);
	SET_VARSIZE(result, VARHDRSZ + length);
	snprintf(result->name, length , "%s", cname);
	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(gcoord_send);

Datum
gcoord_send(PG_FUNCTION_ARGS)
{
	Gcoord    *gcoord = (Gcoord *) PG_GETARG_POINTER(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendstring(&buf, gcoord->name);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


/*****************************************************************************
 * Operator class for defining B-tree index
 *
 * It's essential that the comparison operators and support function for a
 * B-tree index opclass always agree on the relative ordering of any two
 * data values.  Experience has shown that it's depressingly easy to write
 * unintentionally inconsistent functions.  One way to reduce the odds of
 * making a mistake is to make all the functions simple wrappers around
 * an internal three-way-comparison function, as we do here.
 *****************************************************************************/

static int
gcoord_abs_cmp_internal(Gcoord * a, Gcoord * b)
{
	char * new_str1=connocal_form(a->name);
	char * new_str2=connocal_form(b->name);
	char *new_loc_name1 = get_loc_name(new_str1);
	char *new_loc_name2 = get_loc_name(new_str2);
	if (strcmp(new_loc_name1, new_loc_name2)==0)
	{	
		char *new_latit1 = get_latit(new_str1);
		char *new_latit2 = get_latit(new_str2);
		if (strcmp(new_latit1, new_latit2)==0)
		{
			char *new_longi1 = get_longi(new_str1);
			char *new_longi2 = get_longi(new_str2);
			if (strcmp(new_longi1, new_longi2)==0)
			{
				return 0;
			}
		}
	} 
	return 1;
}

static int
gcoord_abs_gcmp_internal(Gcoord * a, Gcoord * b)
{
	char * new_str1=connocal_form(a->name);
	char * new_str2=connocal_form(b->name);
	char *new_latit1 = get_latit(new_str1);
	char *new_latit2 = get_latit(new_str2);
	if (strcmp(new_latit1, new_latit2)==0)
	{
		char *new_longi1 = get_longi(new_str1);
		char *new_longi2 = get_longi(new_str2);
		if (strcmp(new_longi1, new_longi2)==0)
		{
			return 0;
		}
	}
	
	return 1;
}


PG_FUNCTION_INFO_V1(gcoord_abs_lt);

Datum
gcoord_abs_lt(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) < 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_le);

Datum
gcoord_abs_le(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) <= 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_eq);

Datum
gcoord_abs_eq(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) == 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_neq);

Datum
gcoord_abs_neq(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) != 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_ge);

Datum
gcoord_abs_ge(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) >= 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_gt);

Datum
gcoord_abs_gt(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_cmp_internal(a, b) > 0);
}

PG_FUNCTION_INFO_V1(gcoord_abs_cmp);

Datum
gcoord_abs_cmp(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(gcoord_abs_cmp_internal(a, b));
}

PG_FUNCTION_INFO_V1(gcoord_abs_geq);

Datum
gcoord_abs_geq(PG_FUNCTION_ARGS)
{
	Gcoord    *a = (Gcoord *) PG_GETARG_POINTER(0);
	Gcoord    *b = (Gcoord *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(gcoord_abs_gcmp_internal(a, b));
}

PG_FUNCTION_INFO_V1(convert_string);

Datum
convert_string(PG_FUNCTION_ARGS)
{
	Gcoord	   *str = (Gcoord *) PG_GETARG_POINTER(0);

	char *new_str = connocal_form(str->name);
	int length =strlen(new_str);
	text *result = (text *) palloc(VARHDRSZ + length);
	SET_VARSIZE(result, VARHDRSZ + length);
	memcpy(VARDATA(result), new_str,length);
	PG_RETURN_TEXT_P(result);
}

PG_FUNCTION_INFO_V1(convert2dms);

Datum
convert2dms(PG_FUNCTION_ARGS)
{
	Gcoord	   *str = (Gcoord *) PG_GETARG_POINTER(0);
	char *new_str = connocal_form(str->name);
	char *new_loc_name = get_loc_name(new_str);
	char *new_latit = get_latit(new_str);
	new_latit[strcspn(new_latit, "°")] = '\0';
	char *new_longi = get_longi(new_str);
	new_longi[strcspn(new_longi, "°")] = '\0';

	float new_latit1 = atof(new_latit);
	float new_longi1 = atof(new_longi);

	int new_latit11=(int)(new_latit1*10000+0.5);
	int new_longi11=(int)(new_longi1*10000+0.5);
	int latit_degrees = (int)(new_latit11/10000);
	int longi_degrees = (int)(new_longi11/10000);

	int latit_minutes = (int)((new_latit11-latit_degrees*10000)*60/10000);
	int longi_minutes = (int)((new_longi11-longi_degrees*10000)*60/10000);

	int latit_seconds = (int)(3600 * (new_latit11-latit_degrees*10000)/10000-(60*latit_minutes));
	int longi_seconds = (int)(3600 * (new_longi11-longi_degrees*10000)/10000-(60*longi_minutes));
	// int latit_seconds = (int)(((new_latit11-latit_degrees*10000)-((latit_minutes/60)*10000))*3600/10000); 
	// int longi_seconds = (int)(((new_longi11-longi_degrees*10000)-((longi_minutes/60)*10000))*3600/10000);

	
	text* result1 = (text*) palloc(VARHDRSZ+strlen(new_str)+2);
	char* result = (char*) palloc(VARHDRSZ+strlen(new_str)+2);

	strcpy(result, new_loc_name);

	
	strcat(result, ",");
	char strLat[20], strLong[20];
	if (latit_seconds == 0)
	{
		if (latit_minutes == 0)
		{
			sprintf(strLat, "%d°", latit_degrees);
		}
		else
		{
			sprintf(strLat, "%d°%d'", latit_degrees,latit_minutes);
		}
	}
	else if (latit_minutes == 0)
	{
		sprintf(strLat, "%d°%d\"", latit_degrees,latit_seconds);
	}
	else
	{
		sprintf(strLat, "%d°%d'%d\"", latit_degrees,latit_minutes,latit_seconds);
	}
	
	if (longi_seconds == 0)
	{
		if (longi_minutes == 0)
		{
			sprintf(strLong, "%d°", longi_degrees);
		}
		else
		{
			sprintf(strLong, "%d°%d'", longi_degrees,longi_minutes);
		}
	}
	else if (longi_minutes == 0)
	{
		sprintf(strLong, "%d°%d\"", longi_degrees,longi_seconds);
	}
	else
	{
		sprintf(strLong, "%d°%d'%d\"", longi_degrees,longi_minutes,longi_seconds);
	}

	char *latDir = get_latit_dir(new_str);
	char *longDir = get_longi_dir(new_str);
	strcat(result, strLat);
	strncat(result, latDir, 1);
	
	strcat(result, ",");
	strcat(result, strLong);
	strncat(result, longDir, 1);

	SET_VARSIZE(result1, VARHDRSZ + strlen(str->name)+2);
	memcpy(VARDATA(result1), result,strlen(str->name)+2);
	PG_RETURN_TEXT_P(result1);
}

// PG_FUNCTION_INFO_V1(gcoord_hash);

// Datum
// gcoord_hash(PG_FUNCTION_ARGS)
// {
// 	Gcoord *gcoord = (Gcoord *) PG_GETARG_POINTER(0);
// 	char *result = (char *)palloc(VARSIZE_ANY_EXHDR(gcoord));
// 	snprintf(result, VARSIZE_ANY_EXHDR(gcoord), "%s", gcoord->name);
// 	int h_code = DatumGetUInt32(hash_any((const unsigned char *) result, VARSIZE_ANY_EXHDR(gcoord)));
// 	// int h_code = DatumGetUInt32(hash_any((unsigned char *) gcoord->name, strlen(gcoord->name)));
// 	PG_RETURN_INT32(h_code);
// }