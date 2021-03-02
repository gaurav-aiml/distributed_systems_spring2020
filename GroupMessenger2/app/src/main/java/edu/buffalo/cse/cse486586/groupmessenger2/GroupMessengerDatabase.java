package edu.buffalo.cse.cse486586.groupmessenger2;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;


final class GroupMessengerContract {
    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private GroupMessengerContract() {}

    /* Inner class that defines the table contents */
    public static class GroupMessenger implements BaseColumns {
        public static final String TABLE_NAME = "Messages";
        public static final String Key = "key";
        public static final String Value = "value";

    }
}

public class GroupMessengerDatabase extends SQLiteOpenHelper {

    // If you change the database schema, you must increment the database version.
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "GroupMessenger.db";
    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + GroupMessengerContract.GroupMessenger.TABLE_NAME + " (" +
                    GroupMessengerContract.GroupMessenger.Key + " TEXT PRIMARY KEY," +
                    GroupMessengerContract.GroupMessenger.Value + " TEXT)";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + GroupMessengerContract.GroupMessenger.TABLE_NAME;

    public GroupMessengerDatabase(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }
}