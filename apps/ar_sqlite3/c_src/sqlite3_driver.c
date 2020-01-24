#include <stdlib.h>
#include <string.h>
#include "ei.h"
#include "erl_comm.h"
#include "sqlite3.h"

static int handle_msg(char *buf, int *index);
static int handle_atom_msg(char *buf, int *index, const char *atom);
static int handle_tuple_msg(char *buf, int *index, int arity);
static int handle_open_msg(char *buf, int *index);
static int handle_close_msg(char *buf, int *index);
static int handle_prepare_msg(char *buf, int *index);
static int handle_bind_msg(char *buf, int *index);
static int handle_step_msg(char *buf, int *index);
static int handle_reset_msg(char *buf, int *index);
static int handle_finalize_msg(char *buf, int *index);
static int handle_exec_msg(char *buf, int *index);

static int bind_int_list(char *buf, int *index, sqlite3_stmt *stmt, unsigned char *vals, int numVals);
static int bind_term_list(char *buf, int *index, sqlite3_stmt *stmt, int arity);

static int mk_ok_addr_tuple(char *buf, int *index, void *ptr);
static int mk_error_tuple(char *buf, int *index, const char *errmsg, int line);

int main(void)
{
	byte buf[65536];
	int len;
	int *index = &len;

	while (read_cmd(buf) > 0)
	{
		if (handle_msg((char *) buf, index))
		{
			fputs("sqlite3_driver ran into an unexpected error.\n", stderr);
			return EXIT_FAILURE;
		}

		write_cmd(buf, len);
	}

	return EXIT_SUCCESS;
}

static int handle_msg(char *buf, int *index)
{
	int version;
	char atom[MAXATOMLEN];
	int arity;

	*index = 0;

	if (ei_decode_version(buf, index, &version) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_atom(buf, index, atom) == 0)
	{
		return handle_atom_msg(buf, index, atom);
	}

	if (ei_decode_tuple_header(buf, index, &arity) == 0)
	{
		return handle_tuple_msg(buf, index, arity);
	}

	return mk_error_tuple(buf, index, "badmsg", __LINE__);
}

static int handle_atom_msg(char *buf, int *index, const char *atom)
{
	if (strcmp(atom, "libversion") == 0)
	{
		*index = 0;
		if (ei_encode_version(buf, index) != 0) return -1;
		if (ei_encode_string(buf, index, sqlite3_libversion()) != 0) return -1;
		return 0;
	}

	return mk_error_tuple(buf, index, "badmsg", __LINE__);
}

static int handle_tuple_msg(char *buf, int *index, int arity)
{
	char atom[MAXATOMLEN];

	if (arity == 2)
	{
		if (ei_decode_atom(buf, index, atom) == 0)
		{
			if (strcmp(atom, "open") == 0)
			{
				return handle_open_msg(buf, index);
			}

			if (strcmp(atom, "close") == 0)
			{
				return handle_close_msg(buf, index);
			}

			if (strcmp(atom, "step") == 0)
			{
				return handle_step_msg(buf, index);
			}

			if (strcmp(atom, "reset") == 0)
			{
				return handle_reset_msg(buf, index);
			}

			if (strcmp(atom, "finalize") == 0)
			{
				return handle_finalize_msg(buf, index);
			}
		}
	}

	if (arity == 3)
	{
		if (ei_decode_atom(buf, index, atom) == 0)
		{
			if (strcmp(atom, "prepare") == 0)
			{
				return handle_prepare_msg(buf, index);
			}

			if (strcmp(atom, "bind") == 0)
			{
				return handle_bind_msg(buf, index);
			}

			if (strcmp(atom, "exec") == 0)
			{
				return handle_exec_msg(buf, index);
			}
		}
	}

	return mk_error_tuple(buf, index, "badmsg", __LINE__);
}

static int handle_open_msg(char *buf, int *index)
{
	int type, size;
	char *filename;
	sqlite3 *db;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;
	filename = malloc(size + 1);

	if (filename == NULL)
	{
		return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
	}

	if (ei_decode_string(buf, index, filename) != 0)
	{
		free(filename);
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (sqlite3_open(filename, &db) != SQLITE_OK)
	{
		free(filename);
		return mk_error_tuple(buf, index, "open_not_ok", __LINE__);
	}

	free(filename);
	return mk_ok_addr_tuple(buf, index, db);
}

static int handle_close_msg(char *buf, int *index)
{
	int type, size;
	sqlite3 *db;
	long len;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &db, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	switch(sqlite3_close(db))
	{
	case SQLITE_BUSY:
		return mk_error_tuple(buf, index, "busy", __LINE__);

	case SQLITE_OK:
		*index = 0;
		if (ei_encode_version(buf, index) != 0) return -1;
		if (ei_encode_atom(buf, index, "ok") != 0) return -1;
		return 0;

	default:
		return mk_error_tuple(buf, index, "close_not_ok", __LINE__);
	}
}

static int handle_prepare_msg(char *buf, int *index)
{
	int type, size;
	sqlite3 *db;
	long len;
	char *query;
	sqlite3_stmt *stmt;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &db, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;
	query = malloc(size + 1);

	if (query == NULL)
	{
		return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
	}

	if (ei_decode_string(buf, index, query) != 0)
	{
		free(query);
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) != SQLITE_OK)
	{
		free(query);
		return mk_error_tuple(buf, index, "prepare_not_ok", __LINE__);
	}

	free(query);
	return mk_ok_addr_tuple(buf, index, stmt);
}

static int handle_bind_msg(char *buf, int *index)
{
	int type, size;
	sqlite3_stmt *stmt;
	long len;
	int arity;
	unsigned char *vals;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &stmt, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (ei_decode_list_header(buf, index, &arity) == 0)
	{
		return bind_term_list(buf, index, stmt, arity);
	}

	// Lists of integers in the range 0..255 are encoded as strings
	vals = malloc(size + 1);

	if (vals == NULL)
	{
		return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
	}

	if (ei_decode_string(buf, index, (char *) vals) == 0)
	{
		return bind_int_list(buf, index, stmt, vals, size);
	}

	free(vals);
	return mk_error_tuple(buf, index, "badmsg", __LINE__);
}

static int handle_step_msg(char *buf, int *index)
{
	int type, size;
	sqlite3_stmt *stmt;
	long len;
	int column_count;
	long long llCol;
	double dCol;
	const unsigned char *tCol;
	const void *bCol;
	int tColSize, bColSize;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &stmt, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	switch (sqlite3_step(stmt))
	{
	case SQLITE_BUSY:
		return mk_error_tuple(buf, index, "busy", __LINE__);

	case SQLITE_MISUSE:
		return mk_error_tuple(buf, index, "misuse", __LINE__);

	case SQLITE_ROW:
		*index = 0;
		if (ei_encode_version(buf, index) != 0) return -1;
		if (ei_encode_tuple_header(buf, index, 2) != 0) return -1;
		if (ei_encode_atom(buf, index, "row") != 0) return -1;

		column_count = sqlite3_column_count(stmt);
		if (ei_encode_list_header(buf, index, column_count) != 0) return -1;

		for (int i = 0; i < column_count; i++)
		{
			switch (sqlite3_column_type(stmt, i))
			{
			case SQLITE_INTEGER:
				llCol = sqlite3_column_int64(stmt, i);
				if (ei_encode_longlong(buf, index, llCol) != 0) return -1;
				break;

			case SQLITE_FLOAT:
				dCol = sqlite3_column_double(stmt, i);
				if (ei_encode_double(buf, index, dCol) != 0) return -1;
				break;

			case SQLITE_TEXT:
				tCol = sqlite3_column_text(stmt, i);
				tColSize = sqlite3_column_bytes(stmt, i);
				if (ei_encode_binary(buf, index, tCol, tColSize) != 0) return -1;
				break;

			case SQLITE_BLOB:
				bCol = sqlite3_column_blob(stmt, i);
				bColSize = sqlite3_column_bytes(stmt, i);
				if (ei_encode_binary(buf, index, bCol, bColSize) != 0) return -1;
				break;

			case SQLITE_NULL:
				if (ei_encode_atom(buf, index, "null") != 0) return -1;
				break;

			default:
				return -1;
			}
		}

		if (ei_encode_empty_list(buf, index) != 0) return -1;

		return 0;

	case SQLITE_DONE:
		*index = 0;
		if (ei_encode_version(buf, index) != 0) return -1;
		if (ei_encode_atom(buf, index, "done") != 0) return -1;
		return 0;

	default:
		return mk_error_tuple(buf, index, "step_error", __LINE__);
	}
}

static int handle_reset_msg(char *buf, int *index)
{
	int type, size;
	sqlite3_stmt *stmt;
	long len;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &stmt, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (sqlite3_reset(stmt) != SQLITE_OK)
	{
		return mk_error_tuple(buf, index, "reset_not_ok", __LINE__);
	}

	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	return 0;
}

static int handle_finalize_msg(char *buf, int *index)
{
	int type, size;
	sqlite3_stmt *stmt;
	long len;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &stmt, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (sqlite3_finalize(stmt) != SQLITE_OK)
	{
		return mk_error_tuple(buf, index, "finalize_not_ok", __LINE__);
	}

	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	return 0;
}

static int handle_exec_msg(char *buf, int *index)
{
	int type, size;
	sqlite3 *db;
	long len;
	char *query;

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;

	if (size > (int) sizeof(void *))
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_decode_binary(buf, index, &db, &len) != 0)
	{
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (ei_get_type(buf, index, &type, &size) != 0) return -1;
	query = malloc(size + 1);

	if (query == NULL)
	{
		return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
	}

	if (ei_decode_string(buf, index, query) != 0)
	{
		free(query);
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	if (sqlite3_exec(db, query, NULL, NULL, NULL) != SQLITE_OK)
	{
		free(query);
		return mk_error_tuple(buf, index, "exec_not_ok", __LINE__);
	}

	free(query);

	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	return 0;
}

static int bind_int_list(char *buf, int *index, sqlite3_stmt *stmt, unsigned char *vals, int numVals)
{
	for (int i = 0; i < numVals; i++)
	{
		switch (sqlite3_bind_int(stmt, i + 1, vals[i]))
		{
		case SQLITE_OK:
			continue;

		case SQLITE_MISUSE:
			free(vals);
			return mk_error_tuple(buf, index, "misuse", __LINE__);

		default:
			free(vals);
			return mk_error_tuple(buf, index, "bind_error", __LINE__);
		}
	}

	free(vals);

	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	return 0;
}

static int bind_term_list(char *buf, int *index, sqlite3_stmt *stmt, int arity)
{
	int type, size;
	long long llVal;
	double dVal;
	long bValSize = 0;
	int tupleArity;
	char atom[MAXATOMLEN];
	void *dynVal;

	for (int i = 1; i <= arity; i++)
	{
		if (ei_get_type(buf, index, &type, &size) != 0)
		{
			return -1;
		}

		if (ei_decode_longlong(buf, index, &llVal) == 0)
		{
			switch (sqlite3_bind_int64(stmt, i, llVal))
			{
			case SQLITE_OK:
				continue;

			case SQLITE_MISUSE:
				return mk_error_tuple(buf, index, "misuse", __LINE__);

			default:
				return mk_error_tuple(buf, index, "bind_error", __LINE__);
			}
		}

		if (ei_decode_double(buf, index, &dVal) == 0)
		{
			switch (sqlite3_bind_double(stmt, i, dVal))
			{
			case SQLITE_OK:
				continue;

			case SQLITE_MISUSE:
				return mk_error_tuple(buf, index, "misuse", __LINE__);

			default:
				return mk_error_tuple(buf, index, "bind_error", __LINE__);
			}
		}

		// Accept blobs as tuples {blob, <<...>>}
		if (ei_decode_tuple_header(buf, index, &tupleArity) == 0)
		{
			if (tupleArity != 2)
			{
				return mk_error_tuple(buf, index, "badmsg", __LINE__);
			}

			if (ei_get_type(buf, index, &type, &size) != 0) return -1;

			if (size >= MAXATOMLEN || ei_decode_atom(buf, index, atom))
			{
				return mk_error_tuple(buf, index, "badmsg", __LINE__);
			}

			if (strcmp(atom, "blob") != 0)
			{
				return mk_error_tuple(buf, index, "badmsg", __LINE__);
			}

			if (ei_get_type(buf, index, &type, &size) != 0) return -1;
			dynVal = malloc(size);

			if (dynVal == NULL)
			{
				return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
			}

			if (ei_decode_binary(buf, index, dynVal, &bValSize) != 0)
			{
				free(dynVal);
				return mk_error_tuple(buf, index, "badmsg", __LINE__);
			}

			switch (sqlite3_bind_blob(stmt, i, dynVal, bValSize, free))
			{
			case SQLITE_OK:
				continue;

			case SQLITE_MISUSE:
				return mk_error_tuple(buf, index, "misuse", __LINE__);

			default:
				return mk_error_tuple(buf, index, "bind_error", __LINE__);
			}
		}

		// Accept 'null' as an atom
		if (size < MAXATOMLEN && !ei_decode_atom(buf, index, atom))
		{
			if (strcmp(atom, "null") == 0)
			{
				switch(sqlite3_bind_null(stmt, i))
				{
				case SQLITE_OK:
					continue;

				case SQLITE_MISUSE:
					return mk_error_tuple(buf, index, "misuse", __LINE__);

				default:
					return mk_error_tuple(buf, index, "bind_error", __LINE__);
				}
			}

			return mk_error_tuple(buf, index, "badmsg", __LINE__);
		}

		dynVal = malloc(size + 1);

		if (dynVal == NULL)
		{
			return mk_error_tuple(buf, index, "malloc_failed", __LINE__);
		}

		if (ei_decode_string(buf, index, dynVal) == 0)
		{
			switch (sqlite3_bind_text(stmt, i, dynVal, -1, free))
			{
			case SQLITE_OK:
				continue;

			case SQLITE_MISUSE:
				return mk_error_tuple(buf, index, "misuse", __LINE__);

			default:
				return mk_error_tuple(buf, index, "bind_error", __LINE__);
			}
		}

		if (ei_decode_binary(buf, index, dynVal, &bValSize) == 0)
		{
			switch (sqlite3_bind_text(stmt, i, dynVal, bValSize, free))
			{
			case SQLITE_OK:
				continue;

			case SQLITE_MISUSE:
				return mk_error_tuple(buf, index, "misuse", __LINE__);

			default:
				return mk_error_tuple(buf, index, "bind_error", __LINE__);
			}
		}

		free(dynVal);
		return mk_error_tuple(buf, index, "badmsg", __LINE__);
	}

	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	return 0;
}

int mk_ok_addr_tuple(char *buf, int *index, void *ptr)
{
	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_tuple_header(buf, index, 2) != 0) return -1;
	if (ei_encode_atom(buf, index, "ok") != 0) return -1;
	if (ei_encode_binary(buf, index, &ptr, sizeof(void *)) != 0) return -1;
	return 0;
}

int mk_error_tuple(char *buf, int *index, const char *errmsg, int line)
{
	*index = 0;
	if (ei_encode_version(buf, index) != 0) return -1;
	if (ei_encode_tuple_header(buf, index, 3) != 0) return -1;
	if (ei_encode_atom(buf, index, "error") != 0) return -1;
	if (ei_encode_atom(buf, index, errmsg) != 0) return -1;
	if (ei_encode_longlong(buf, index, line) != 0) return -1;
	return 0;
}
