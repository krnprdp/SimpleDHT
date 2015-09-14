package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Programming Assignment 23
 * <p/>
 * -- Pradeep Kiran Chakkirala
 * Person # 50134080, pchakkir (at) buffalo (dot) edu
 * <p/>
 * Some part of the code is taken from the PA1 template
 */

public class SimpleDhtActivity extends Activity {

    String myPort = "";

    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";

    public static String nodeID = "";
    public static SortedMap<String, Integer> nodeList;
    public static ArrayList<Integer> localList;
    public static SortedMap<String, Integer> nL;
    public static int successor = 5554;
    public static int predecessor = 5554;
    public static int myNode = 0;
    public static int flag = 0;
    TextView tv;

    public static String AUTHORITY = "edu.buffalo.cse.cse486586.simpledht.provider";
    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);

        localList = new ArrayList<Integer>();
        nodeList = new TreeMap<String, Integer>(
                new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                });

        try {
            nodeList.put(genHash(Integer.toString(5554)), 5554);
        } catch (NoSuchAlgorithmException e) {
            Log.d("SimpleDhtActivity -- ", " Error with genhash()");
        }

        localList.addAll(nodeList.values());

        //Calculate the port number on which the avd listens on
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        nodeID = portStr;
        myNode = Integer.parseInt(nodeID);

        //If there is only one node
        successor = myNode;
        predecessor = myNode;

        Log.d("------Started Node: ", nodeID);
        //For Debugging
        //Toast.makeText(this,nodeID,Toast.LENGTH_SHORT).show();

        //create the ServerSocket
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e("Error", "Can't create a ServerSocket");
            return;
        }

        //Send the nodeID/Join request to 5554
        if (Integer.parseInt(nodeID) != 5554)
            new JoinTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeID, myPort);

        tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));


        Button btn = (Button) findViewById(R.id.button1);
        btn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(getApplicationContext(), "Successor is " + Integer.toString(successor) +
                        "\nPredecessor is " + Integer.toString(predecessor), Toast.LENGTH_SHORT).show();
            }
        });

    }

    //Sets Message on the screen
    public void setText(String s) {
        tv.append(s + "\n");
    }

    //This is run on all emulators other than 5554 once they receive List Of Nodes from 5554 everytime a new node Joins
    public void updateLocalList(ArrayList list) {

        localList = list;

        for (int i = 0; i < localList.size(); i++) {
            if (localList.get(i) == myNode) {
                if (localList.size() == 1) {
                    successor = localList.get(i);
                    predecessor = localList.get(i);
                    break;
                }

                if (i == 0) {
                    successor = localList.get(i + 1);
                    predecessor = localList.get(localList.size() - 1);
                    break;
                }

                if (i == localList.size() - 1) {
                    successor = localList.get(0);
                    predecessor = localList.get(i - 1);
                    break;
                }

                if (i > 0 && i < localList.size()) {
                    successor = localList.get(i + 1);
                    predecessor = localList.get(i - 1);
                    break;
                }
            }
        }

        nL = new TreeMap<String, Integer>(
                new Comparator<String>() {
                    @Override
                    public int compare(String o1, String o2) {
                        return o1.compareTo(o2);
                    }
                });
        ArrayList<Integer> list2 = new ArrayList<Integer>();
        list2 = SimpleDhtActivity.localList;
        try {

            for (int i = 0; i < list2.size(); i++)
                nL.put(genHash(Integer.toString(list2.get(i))), list2.get(i));
        } catch (NoSuchAlgorithmException e) {
            Log.d("SimpleDhtActivity -- ", " Error with genhash()");
        }


    }

    //This is run only on emulator 5554 after a new node Joins
    public void updateMainList(int node) {

        try {
            nodeList.put(genHash(Integer.toString(node)), node);
        } catch (NoSuchAlgorithmException e) {
            Log.d("SimpleDhtActivity -- ", " Error with genhash() in updateMainList()");
        }

        ArrayList<Integer> a = new ArrayList<Integer>();
        nL = nodeList;
        a.addAll(nodeList.values());

        localList = a;

        for (int i = 0; i < localList.size(); i++) {
            if (localList.get(i) == myNode) {
                if (localList.size() == 1) {
                    successor = localList.get(i);
                    predecessor = localList.get(i);
                    break;
                }

                if (i == 0) {
                    successor = localList.get(i + 1);
                    predecessor = localList.get(localList.size() - 1);
                    break;
                }

                if (i == localList.size() - 1) {
                    successor = localList.get(0);
                    predecessor = localList.get(i - 1);
                    break;
                }

                if (i > 0 && i < localList.size()) {
                    successor = localList.get(i + 1);
                    predecessor = localList.get(i - 1);
                    break;
                }
            }
        }

    }

    public static void sendToSucc(String[] a) {
        new AsyncTask<String, Void, Void>() {
            @Override
            protected Void doInBackground(String... msgs) {
                try {
                    Log.v("SendToSucc", msgs[1] + " key " + msgs[0] + " value " + msgs[2]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]) * 2);
                    ContentValues cv = new ContentValues();
                    cv.put("key", msgs[1]);
                    cv.put("value", msgs[2]);
                    Message m = new Message("SendToSucc", msgs[1], msgs[2]);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(m);
                    Log.v("SendToSucc", "Sent");
                    socket.close();
                } catch (UnknownHostException e) {
                    Log.e("Error", "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e("Error", e.toString());
                }
                return null;
            }
        }.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, a[2], a[0], a[1]);
        // new SendToSuccTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Integer.toString(SimpleDhtActivity.successor), a[1], a[2]);
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Message m = null;
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                    m = (Message) ois.readObject();
                    Log.v("+++", m.flag);
                    if (m.flag.equals("Join")) {
                        int node = m.nodeID;
                        publishProgress("Node added: " + Integer.toString(node));
                        updateMainList(node);
                        new UpdateListTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodeID, myPort);
                    }

                    if (m.flag.equals("UpdateList")) {
                        updateLocalList(m.list);
                        publishProgress("Received List Update");
                    }

                    if (m.flag.equals("SendToSucc")) {
                        Log.v("+++", "Received from pred");
                        ContentValues cv = new ContentValues();
                        cv.put("key", m.key);
                        cv.put("value", m.value);
                        try {
                            flag = 1;

                            getContentResolver().insert(CONTENT_URI, cv);

                            flag = 0;
                        } catch (NullPointerException e) {
                            Log.d("$$$$$$$$$$$$$$$$$", m.key + "  " + m.value);
                        }
                        publishProgress("Received Insert from another node");
                    }

                    if (m.flag.equals("Query")){
                        publishProgress("Received Query Request from another node");
                        Cursor resultCursor = getContentResolver().query(CONTENT_URI, null,
                                m.selection, null, null);
//
//                        for(int i=0;i<resultCursor.getCount();i++){
//                            if (m.selection == resultCursor.getString(resultCursor.getColumnIndex("value")));
//                        }
                        resultCursor.moveToFirst();
                        publishProgress(resultCursor.getString(resultCursor.getColumnIndex("value")));
                        Message ret = new Message(resultCursor.getString(resultCursor.getColumnIndex("value")));
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(ret);

                    }

                    if(m.flag.equals("QueryStar")){
                        publishProgress("Received QueryStar Request!!");
                        Cursor resultCursor = getContentResolver().query(CONTENT_URI, null,
                                "@", null, null);
                        resultCursor.moveToFirst();
                        HashMap<String,String> hm = new HashMap<String,String>();
                        while(!resultCursor.isAfterLast()){
                            String key = resultCursor.getString(resultCursor.getColumnIndex("key"));
                            String value = resultCursor.getString(resultCursor.getColumnIndex("value"));
                            hm.put(key,value);
                            resultCursor.moveToNext();
                        }

                        Message ret = new Message(hm);
                        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                        oos.writeObject(ret);

                    }

                }
            } catch (IOException ioe) {
                Log.d("IOException", ioe.toString());
            } catch (Exception e) {
                Log.d("Exception", e.toString());
            }
            Log.v("///////////", "server task exited");
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            String strReceived = strings[0].trim();
            setText(strReceived);
            return;
        }
    }

    private class JoinTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                Message m = new Message("Join", Integer.parseInt(msgs[0]));
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(m);
                socket.close();
            } catch (UnknownHostException e) {
                Log.e("Error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Error", e.toString());
            }
            return null;
        }
    }

    private class UpdateListTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String remotePort = "";
                for (int i = 1; i < 5; i++) {
                    switch (i) {
                        case 1:
                            remotePort = REMOTE_PORT1;
                            break;
                        case 2:
                            remotePort = REMOTE_PORT2;
                            break;
                        case 3:
                            remotePort = REMOTE_PORT3;
                            break;
                        case 4:
                            remotePort = REMOTE_PORT4;
                    }
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
                    Message m = new Message("UpdateList", localList);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.writeObject(m);
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.e("Error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Error", e.toString());
            }
            return null;
        }
    }

    private class SendToSuccTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.v("SendToSucc", msgs[0]);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0]));
                ContentValues cv = new ContentValues();
                cv.put("key", msgs[1]);
                cv.put("value", msgs[2]);
                Message m = new Message("SendToSucc", msgs[1], msgs[2]);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(m);
                socket.close();
            } catch (UnknownHostException e) {
                Log.e("Error", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Error", e.toString());
            }
            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        Log.d("^^^^^^^^^^", input);
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


//End of SimpleDhtActivity
}
