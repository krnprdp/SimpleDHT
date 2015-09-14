package edu.buffalo.cse.cse486586.simpledht;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Pradeep on 3/29/15.
 * <p/>
 * Referenced from android tutorials on thenewboston.org
 */
public class DBHelper extends SQLiteOpenHelper {

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String DATABASE_NAME = "database";
    public static final String TABLE_NAME = "myTable";
    public static final int DATABASE_VERSION = 1;

    public String getDatabaseTable() {
        return TABLE_NAME;
    }

    public String getDatabase() {
        return DATABASE_NAME;
    }

    public DBHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {

        db.execSQL("CREATE TABLE " + TABLE_NAME + " (" +
                        KEY_FIELD + " TEXT PRIMARY KEY, " +
                        VALUE_FIELD + " TEXT);"
        );
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(db);
    }
}
