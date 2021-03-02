package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.provider.BaseColumns;

final class SimpleDhtContract {
    private SimpleDhtContract() {};

    public static class SimpleDhtDB implements BaseColumns {
        public static final String TABLE_NAME = "SimpleDhtMessages";
        public static final String Key = "key";
        public static final String Value = "value";
    }
}

public class SimpleDhtDatabase extends SQLiteOpenHelper{

    public static final int DATABASE_VERSION = 2;
    public static final String DATABASE_NAME = "SimpleDht.db";
    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE "+SimpleDhtContract.SimpleDhtDB.TABLE_NAME+"("+SimpleDhtContract.SimpleDhtDB.Key+" TEXT PRIMARY KEY," +SimpleDhtContract.SimpleDhtDB.Value + " TEXT)";

    private static final String SQL_DELETE_ENTRIES = "DROP TABLE IF EXISTS " + SimpleDhtContract.SimpleDhtDB.TABLE_NAME;

    public SimpleDhtDatabase(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
    db.execSQL(SQL_DELETE_ENTRIES);
    onCreate(db);
    }

    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }
}
