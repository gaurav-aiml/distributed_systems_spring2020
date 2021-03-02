package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDhtActivity extends Activity {

    private TextView tv;
    private ContentResolver contentResolver;
    private Uri uri;


    @Override
    protected void onCreate(Bundle savedInstanceState) {

        /*
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }
*/
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_simple_dht_main);
    Button globalTest, localTest, localDelete, globalDelete;
    contentResolver = getContentResolver();
    globalTest = (Button) findViewById(R.id.button2);
    localTest = (Button) findViewById(R.id.button1);
    globalDelete = (Button) findViewById(R.id.button5);
    localDelete = (Button) findViewById(R.id.button4);

    Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
    uri = uriBuilder.build();

        globalTest.setOnClickListener(new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            Cursor resultCursor = contentResolver.query(uri, null,
                    "*", null, null);
            displayCursorOnTextView(resultCursor);
        }
    });

        localTest.setOnClickListener(new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            Cursor resultCursor = contentResolver.query(uri, null,
                    "@", null, null);
            displayCursorOnTextView(resultCursor);
        }
    });

        localDelete.setOnClickListener(new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            int resp = contentResolver.delete(uri, "@", null);
            tv.append("Local Delete Response: " + Integer.toString(resp) + "\n");
        }
    });

        globalDelete.setOnClickListener(new View.OnClickListener() {
        @Override
        public void onClick(View view) {
            int resp = contentResolver.delete(uri, "*", null);
            tv.append("Global Delete Response: " + Integer.toString(resp) + "\n");
        }
    });

    tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
    findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
}

    public void displayCursorOnTextView(Cursor cursor) {
        Log.d("Cursor Size:", Integer.toString(cursor.getCount()));
        if (cursor.moveToFirst()) {
            while (!cursor.isAfterLast()) {
                tv.append(cursor.getString(0) + ":" + cursor.getString(1) + "\n");
                cursor.moveToNext();
            }
        } else {
            tv.append("Empty Result returned!\n");
        }
        cursor.close();
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
