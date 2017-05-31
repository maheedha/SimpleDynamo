package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import android.annotation.TargetApi;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;
import static java.net.InetAddress.getByAddress;

public class SimpleDynamoProvider extends ContentProvider {
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] port_array = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static String myPort = null;

    static final int SERVER_PORT = 10000;

    private List<String> hashedPortSorted = new ArrayList<String>();
    private List<String> keyValue = new ArrayList<String>();


    int listSize = 0;
    int currentPort = 0;
    String curr = null;
    Map<String, Integer> hm = new HashMap<String, Integer>();


    Node Port = new Node();

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        String[] files;
        Context ctx = getContext();
        files = ctx.fileList();
        int lcl[] = new int[3];
        if (selection.equals("@")) {
            for (String s : files) {
                ctx.deleteFile(s);
            }
        } else {
            lcl = findPort(selection);
            for (int i = 0; i < lcl.length; i++) {
                if (Integer.parseInt(myPort) == lcl[i]) {
                    ctx.deleteFile(selection);
                    continue;
                }
            }
            StringBuilder msg = new StringBuilder();
            msg.append("delete");
            msg.append(",");
            msg.append(selection);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(lcl[0]), Integer.toString(lcl[1]), Integer.toString(lcl[2]));

        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public void saveInternal(String key, String value) {
        Context ctx = getContext();
        File file = new File(ctx.getFilesDir(), key);
        FileOutputStream fos = null;
        try {
            fos = ctx.openFileOutput(key, Context.MODE_PRIVATE);
            fos.write(value.getBytes());
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int[] findPort(String key) {
        String hashedKey = null;
        int[] lcl = new int[3];
        try {
            hashedKey = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        if (hashedKey.compareTo(hashedPortSorted.get(hashedPortSorted.size() - 1)) > 0) {
            lcl[0] = hm.get(hashedPortSorted.get(0));
            lcl[1] = hm.get(hashedPortSorted.get(1));
            lcl[2] = hm.get(hashedPortSorted.get(2));
        } else {
            for (int i = 0; i < hashedPortSorted.size(); i++) {
                if (hashedKey.compareTo(hashedPortSorted.get(i)) < 0) {
                    if (i == 4) {
                        lcl[0] = hm.get(hashedPortSorted.get(4));
                        lcl[1] = hm.get(hashedPortSorted.get(0));
                        lcl[2] = hm.get(hashedPortSorted.get(1));
                    } else if (i == 3) {
                        lcl[0] = hm.get(hashedPortSorted.get(3));
                        lcl[1] = hm.get(hashedPortSorted.get(4));
                        lcl[2] = hm.get(hashedPortSorted.get(0));
                    } else {
                        lcl[0] = hm.get(hashedPortSorted.get(i));
                        lcl[1] = hm.get(hashedPortSorted.get(i + 1));
                        lcl[2] = hm.get(hashedPortSorted.get(i + 2));
                    }
                    break;
                }

            }
        }
        return lcl;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        // TODO Auto-generated method s
        Context ctx = getContext();
        String key = (String) values.get("key");
        String value = (String) values.get("value");
        int lcl[] = null;
        String currPort = null;
        try {
            currPort = genHash(curr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (values.containsKey("insert")) {
            saveInternal(key, value);
        } else {
            lcl = findPort(key);
            StringBuilder msg = new StringBuilder();
            msg.append("insert");
            msg.append(",");
            msg.append(key);
            msg.append(",");
            msg.append(value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), String.valueOf(lcl[0]), String.valueOf(lcl[1]), String.valueOf(lcl[2]));

        }
        return uri;
    }


    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String ack = "OK";
            ContentValues cv = new ContentValues();
            Socket server = null;
            Uri mUri;
            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

            while (true) {
                try {
                    try {
                        server = serverSocket.accept();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    PrintWriter out =
                            new PrintWriter(server.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(server.getInputStream()));

                    String input = in.readLine();
                    if (input != null) {
                        String[] str_array = input.split(",");

                        if (str_array[0].equals("insert")) {
                            cv = new ContentValues();
                            cv.put(KEY_FIELD, str_array[1]);
                            cv.put(VALUE_FIELD, str_array[2]);
                            cv.put("insert", "insert");
                            //ContentResolver cr = getContext().getContentResolver();
                            insert(mUri, cv);
                            out.println("ok");
                            server.close();
                        } else if (str_array[0].equals("delete")) {

                            getContext().deleteFile(str_array[1]);
                            out.println("ok");
                            server.close();
                        } else if (str_array[0].equals("call4")) {
                            String value = null;
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < keyValue.size(); i++) {
                                Log.i("MissedKeys", keyValue.get(i));
                                sb.append(keyValue.get(i));
                                sb.append(":");
                            }
                            out.println(sb.toString());
                            keyValue.clear();
                            sb.setLength(0);
                            server.close();
                        } else if (str_array[0].equals("routeQuery")) {
                            FileInputStream fis = getContext().openFileInput(str_array[1]);
                            InputStreamReader ini = new InputStreamReader(fis);
                            BufferedReader br = new BufferedReader(ini);
                            String value = br.readLine();
                            out.println(value);
                            server.close();
                        } else if (str_array[0].equals("*query")) {
                            String[] files;
                            files = getContext().fileList();
                            String value = null;
                            StringBuilder msgs = new StringBuilder();

                            for (String s : files) {
                                try {
                                    FileInputStream fis = getContext().openFileInput(s);
                                    InputStreamReader inpu = new InputStreamReader(fis);
                                    BufferedReader br = new BufferedReader(inpu);
                                    value = br.readLine();
                                    msgs.append(s);
                                    msgs.append(",");
                                    msgs.append(value);
                                    msgs.append(":");
                                } catch (FileNotFoundException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                            out.println(msgs.toString());
                            if(in.readLine()=="ok")server.close();
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {

        @Override
        protected String doInBackground(String... msgs) {
            String value = " ";
            try {
                String msgToSend = msgs[0];
                String[] str_array = msgToSend.split(",");
                Socket socket = null;
                ContentValues cv = new ContentValues();
                Uri mUri;
                mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

                if (str_array[0].equals("insert")) {
                    List<String> llist = new ArrayList<String>(Arrays.asList(msgs[1], msgs[2], msgs[3]));

                    for (int index = 0; index < llist.size(); index++) {
                        socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(llist.get(index)));
                        Log.i("insert", llist.get(index));
                        PrintWriter out =
                                new PrintWriter(socket.getOutputStream(), true);
                        BufferedReader in =
                                new BufferedReader(
                                        new InputStreamReader(socket.getInputStream()));
                        out.println(msgToSend);
                        try {
                            if (in.readLine().equals("ok")) {
                                socket.close();
                            }
                        } catch (NullPointerException e) {

                            Log.i("insertexception", String.valueOf(Integer.parseInt(llist.get(index))));
                            keyValue.add(str_array[1] + "," + str_array[2]);
                            Log.i("LateKey", str_array[1]);
                            Log.i("LateKey", str_array[2]);
                        }

                    }
                } else if (str_array[0].equals("call4")) {
                    Log.i("call4", "call4");
                    List<Integer> serverList = new ArrayList<Integer>();
                    String temp[] = null;
                    String temp1[] = null;
                    for (int i = 0; i < port_array.length; i++) {
                        if (myPort == port_array[i]) {

                        } else {

                            socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port_array[i]));
                            Log.i("call4", port_array[i]);
                            PrintWriter out =
                                    new PrintWriter(socket.getOutputStream(), true);
                            BufferedReader in =
                                    new BufferedReader(
                                            new InputStreamReader(socket.getInputStream()));
                            out.println("call4,asd");
                            Log.i("call4", "asd");
                            String val = in.readLine();
                            if (val.length() > 0) {
                                temp = val.split(":");
                                for (int j = 0; j < temp.length; j++) {
                                    temp1 = temp[j].split(",");
                                    Log.i("retainedKey", temp1[0]);
                                    Log.i("retainedValue", temp1[1]);
                                    cv = new ContentValues();
                                    cv.put(KEY_FIELD, temp1[0]);
                                    cv.put(VALUE_FIELD, temp1[1]);
                                    insert(mUri, cv);
                                }
                            }
                            socket.close();
                        }
                    }
                } else if (str_array.equals("delete")) {
                    List<String> llist = new ArrayList<String>(Arrays.asList(msgs[1], msgs[2], msgs[3]));
                    for (int index = 0; index < llist.size(); index++) {
                        try {
                            socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(llist.get(index)));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        PrintWriter out =
                                null;
                        try {
                            out = new PrintWriter(socket.getOutputStream(), true);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        BufferedReader in =
                                null;
                        try {
                            in = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        out.println(msgToSend);

                        try {
                            if (in.readLine().equals("ok")) {
                                socket.close();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

                } else if (str_array[0].equals("*query")) {
                    try {
                        socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(msgs[1]));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    PrintWriter out = null;
                    try {
                        out = new PrintWriter(socket.getOutputStream(), true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    out.println(msgToSend);
                    try {
                        BufferedReader in =
                                new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        String val = in.readLine();
                        String str_arr[] = val.split("/");
                        if (str_arr.length > 1)
                            value = str_arr[1];
                         return value;
                    } catch (Exception e) {
                        return null;
                    }
                }
                 else if (str_array[0].equals("routeQuery")) {
                    int port[] = findPort(str_array[1]);
                    Log.i("CurrentPort", Integer.toString(currentPort));
                    try {
                        socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                                port[0]);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    PrintWriter out = null;
                    try {
                        out = new PrintWriter(socket.getOutputStream(), true);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    out.println(msgToSend);
                    out.flush();
                    try {
                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        value = in.readLine();
                        if(!(value.equals("")))
                        { return value;}

                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (NullPointerException e) {
                        try {

                            Log.i("CurrentPortInException", Integer.toString(currentPort));
                            Log.i("CrashedServer", Integer.toString(port[0]));
                            Log.i("CNextServer", Integer.toString(port[1]));
                            socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                                    port[1]);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        PrintWriter bus = null;
                        try {
                            bus = new PrintWriter(socket.getOutputStream(), true);
                        } catch (IOException e2) {
                            e.printStackTrace();
                        }
                        Log.i("MessageToServer",msgToSend);
                        bus.println(msgToSend);
                        bus.flush();

                        BufferedReader in = null;
                        try {
                            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        try {
                            value = in.readLine();
                            Log.i("nextservervalue",value);
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                        return value;
                    }

                }

            } catch (Exception e) {
                Log.i("client final exception", "Exception");
            }
            return value;
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        currentPort = Integer.parseInt(myPort) / 2;
        curr = Integer.toString(currentPort);

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        for (int i = 0; i < 5; i++) {
            int temp = Integer.parseInt(port_array[i]) / 2;
            String node = Integer.toString(temp);
            try {
                hm.put(genHash(node), Integer.parseInt(port_array[i]));
                hashedPortSorted.add(genHash(node));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        Collections.sort(hashedPortSorted);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "call4,servers");

        return false;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        String[] files;
        Context ctx = getContext();
        files = ctx.fileList();
        String[] columnNames = {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(columnNames);
        String value = null;
        if (selection.equals("@")) {
            for (String s : files) {
                try {
                    FileInputStream fis = ctx.openFileInput(s);
                    InputStreamReader in = new InputStreamReader(fis);
                    BufferedReader br = new BufferedReader(in);
                    value = br.readLine();

                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] columnNames1 = {s, value};
                cursor.addRow(columnNames1);
            }
        } else if (selection.equals("*")) {
            StringBuilder msg = new StringBuilder();
            msg.append("*query");
            msg.append(",");
            msg.append(selection);
            String val = null;
            for (int i = 0; i < port_array.length; i++) {

                try {
                    Socket socket = null;
                    socket = new Socket(getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port_array[i]));
                    PrintWriter out = null;
                    out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msg);
                    out.flush();
                    BufferedReader in =
                            new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    val = in.readLine();
                    if (val == null) continue;
                    Log.i("value",val);
                    String str_array[] = val.split(":");
                    if (str_array.length > 0) {
                        Log.i("value_1",str_array.toString());
                        for (int j = 0; j < str_array.length; j++) {
                            if (str_array[j].isEmpty()) continue;
                            String[] str_array1 = str_array[j].split(",");
                            String[] columnNames4 = {str_array1[0], str_array1[1]};
                            cursor.addRow(columnNames4);

                        }


                    }
                    out.println("ok");

                } catch (Exception e) {
                    Log.i("exception_1","exception");
                    return null;
                }

            }
            return cursor;
        }
        else {
                StringBuilder msg = new StringBuilder();
                msg.append("routeQuery");
                msg.append(",");
                msg.append(selection);
                String val = null;
                int port[]=findPort(selection);
                try {
                    val = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), Integer.toString(port[0])).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

                String[] columnNames4 = {selection, val};
                cursor.addRow(columnNames4);

            }
        return cursor;
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
}


