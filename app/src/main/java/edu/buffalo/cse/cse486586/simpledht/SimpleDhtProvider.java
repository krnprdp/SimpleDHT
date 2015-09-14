package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    DBHelper helper;
    private SQLiteDatabase sqlDB;
    public static String AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);
    public static String ret = "";
    public static MatrixCursor mc;
    public static int testSize = 0;
    public static HashMap<String, String> finalMap;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        sqlDB.execSQL("delete from "+DBHelper.TABLE_NAME);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        //When There is only 1 node
        if (SimpleDhtActivity.localList.size() == 1) {
            long row = sqlDB.insert("myTable", null, values);
            if (row > 0) {
                Log.v("insert in case of 1 node", (String) values.get(DBHelper.KEY_FIELD));
                return uri;
            } else
                return null;
        } else { //There are more than 1 nodes

            String hashOfKey = "";
            String hashOfNode = "";

            String hashOfPred = "";


            try {
                hashOfKey = genHash((String) values.get("key"));
                hashOfNode = genHash(SimpleDhtActivity.nodeID);

                hashOfPred = genHash(Integer.toString(SimpleDhtActivity.predecessor));

            } catch (NoSuchAlgorithmException e) {
                Log.d("SimpleDhtProvider---------", "No such algorithm exception");
                e.printStackTrace();
            }

            if (SimpleDhtActivity.flag == 1) {
                long row = sqlDB.insert("myTable", null, values);
                if (row > 0) {
                    Log.v("insert after comparing hash value", (String) values.get(DBHelper.KEY_FIELD));
                    return uri;
                } else
                    return null;
            }


            //Values Can be inserted in this node
            if (hashOfKey.compareTo(hashOfPred) > 0 && hashOfKey.compareTo(hashOfNode) <= 0) {
                long row = sqlDB.insert("myTable", null, values);
                if (row > 0) {
                    Log.v("insert after comparing hash value", (String) values.get(DBHelper.KEY_FIELD));
                    return uri;
                } else
                    return null;

            } else {  //Values need to be sent to the successor node
                Log.v("has to send to successor", "---------");
                String[] cv = new String[3];
                cv[0] = (String) values.get("key");
                cv[1] = (String) values.get("value");
//                Log.v("?|?|?|?|?|?|?|?|?", "key " + cv[0] + " value " + cv[1]);
                SimpleDhtActivity.nL.put(hashOfKey, 1);
                ArrayList<Integer> al = new ArrayList<Integer>();
                al.addAll(SimpleDhtActivity.nL.values());
                SimpleDhtActivity.nL.remove(hashOfKey);
                int search = 1;
                int next = 0;
                for (int h = 0; h < al.size(); h++) {
                    if (al.get(h) == search) {
                        if (h == al.size() - 1) {
                            next = al.get(0);
                        } else
                            next = al.get(h + 1);
                        break;
                    }
                }
//                Log.v("?|?|?|?|?|?|?|?|?", "to " + Integer.toString(next) + " key " + cv[0] + " value " + cv[1]);

                cv[2] = Integer.toString(next);
                SimpleDhtActivity.sendToSucc(cv);
            }
        }

        return null;


    }

    @Override
    public boolean onCreate() {


        helper = new DBHelper(getContext());
        sqlDB = helper.getWritableDatabase();

        if (sqlDB == null)
            return false;
        else
            return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.d("+++++++++++++++", selection + "-----------" + sortOrder);


        //if there is only one node
        if (SimpleDhtActivity.localList.size() == 1) {
            if (selection.contains("*") || selection.contains("@")) {
                Log.d("/////////////", "Does it get here?");
                Cursor cursor = sqlDB.query(DBHelper.TABLE_NAME, null, null, null, null, null, null);
                Log.d("No. of rows ret", Integer.toString(cursor.getCount()));
                return cursor;
            } else {
                Log.d("/\\\\\\\\\\\\\\\\\\//", "here");
               // Cursor cursor = sqlDB.query(DBHelper.TABLE_NAME, null, null, null, null, null, null);
                Cursor cursor = sqlDB.rawQuery("Select key, value from myTable where key = '" + selection + "'", null);
                Log.d("No. of rows ret", Integer.toString(cursor.getCount()));
                return cursor;
            }
        }

        if (selection.contains("@")) {
            Log.d("/////////////", "Selection has @");
            Cursor cursor = sqlDB.query(DBHelper.TABLE_NAME, null, null, null, null, null, null);
            Log.d("No. of rows ret", Integer.toString(cursor.getCount()));
            return cursor;
        }

        if (selection.contains("*")) {
            Thread t = new Thread(new queryStarThread());
            t.start();

            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            Log.d("!!!!!!TEST SIZE!!!!!!", Integer.toString(testSize));

            MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});

            Iterator it = finalMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                mc.newRow().add(pair.getKey()).add(pair.getValue());
            }

            return mc;
        }


        String hashOfKey = "";
        String hashOfNode = "";
        String hashOfPred = "";


        try {
            hashOfKey = genHash(selection);
            hashOfNode = genHash(SimpleDhtActivity.nodeID);
            hashOfPred = genHash(Integer.toString(SimpleDhtActivity.predecessor));
        } catch (NoSuchAlgorithmException e) {
            Log.d("SimpleDhtProvider---------", "No such algorithm exception");
            e.printStackTrace();
        }

        if (hashOfKey.compareTo(hashOfPred) > 0 && hashOfKey.compareTo(hashOfNode) <= 0) {
            Cursor cursor = sqlDB.rawQuery("Select key, value from myTable where key = '" + selection + "'", null);
            //sqlDB.query(DBHelper.TABLE_NAME, null, null, null, null, null, null);
            Log.d("----------------------", Integer.toString(cursor.getCount()));
            return cursor;
        } else {

            Cursor cursor = sqlDB.rawQuery("Select key, value from myTable where key = '" + selection + "'", null);
            Log.d("@@@@@@@@@@@@@", Integer.toString(cursor.getCount()));
            if (cursor.getCount() != 0)
                return cursor;

            SimpleDhtActivity.nL.put(hashOfKey, 1);
            ArrayList<Integer> al = new ArrayList<Integer>();
            al.addAll(SimpleDhtActivity.nL.values());
            SimpleDhtActivity.nL.remove(hashOfKey);
            int search = 1;
            int next = 0;
            for (int h = 0; h < al.size(); h++) {
                if (al.get(h) == search) {
                    if (h == al.size() - 1) {
                        next = al.get(0);
                    } else
                        next = al.get(h + 1);
                    break;
                }
            }
            Log.d("()()()()()()()()(", "Query thread req");
            Thread t = new Thread(new queryThread(selection, next));
            t.start();

            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Log.d("()()()()()()()()(", "Query thread ret");
            MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
            mc.newRow().add(selection).add(ret);
            Log.d("()()()()()()()()(", mc.getColumnName(1));
            return mc;

        }

    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public class queryThread implements Runnable {

        int port;
        String selection;

        public queryThread(String s, int p) {
            this.port = p;
            this.selection = s;
        }

        @Override
        public void run() {

            Message m = new Message("Query", selection);

            Log.d("()()()()()()()()(", "Query thread");
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port * 2);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(m);

                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Message ret2 = (Message) ois.readObject();

                Log.d("->->->->->->->->->", ret2.value);
                ret = ret2.value;


                socket.close();


            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }

    public class queryStarThread implements Runnable {

        int port;
        String selection;

        @Override
        public void run() {

            Message m = new Message("QueryStar", selection);
            finalMap = new HashMap<String, String>();
            Log.d("()()()()()()()()(", "Query Star thread");
            try {

                for (int i = 0; i < SimpleDhtActivity.localList.size(); i++) {
                    port = SimpleDhtActivity.localList.get(i);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port * 2);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(m);

                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    Message ret2 = (Message) ois.readObject();

                    Log.d("->->->->->->->->->", Integer.toString(i));

                    finalMap.putAll(ret2.map);

                    socket.close();
                }

                testSize = finalMap.size();

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }


}
