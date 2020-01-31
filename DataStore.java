import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

class KeyValueStore{
    String filePath;
    FileWriter fileWriter = null;
    FileReader fileReader = null;

    KeyValueStore() throws IOException{
        //initialize default file path
        this.filePath = "store_"+System.currentTimeMillis()+".txt";
        initializeFileContent();
    }

    KeyValueStore(String filePath) throws IOException{
        this.filePath = filePath;
        File file = new File(this.filePath);

        if (!file.exists()) {
            // Create a new file if not exists.
            file.createNewFile();
        }
        initializeFileContent();
    }

    void initializeFileContent()throws IOException{
        writeFileContent("{}");
    }

    void create(String key,JSONObject value)throws IOException,JSONException{
        create(key,value,-1);
    }

    void create(String key, JSONObject value, int timeToLive)throws IOException, JSONException {
        //key and value constraints
        if(key.length() > 32){
            System.out.println("Key length should not exceed 32 characters");
            return;
        }

        if(getJSONObjectSizeInKB(value) > 16){
            System.out.println("JSONObject value size should not exceed 16 KB");
            return;
        }

        //check whether the key is already present
        JSONObject obj = read(key);
        if(obj.length() > 0){
            System.out.println("Given key is already exist");
            return;
        }

        //writes the key value pair into file
        //System.out.println(getFileContent());
        JSONObject fullKeyValueStore = new JSONObject(getFileContent());
        fullKeyValueStore.put(key,value);
        writeFileContent(fullKeyValueStore.toString());

        if(timeToLive != -1){
            //create schedular
            setExpireTimeForKey(key,timeToLive);
        }

        System.out.println("Created!!!");

    }

    JSONObject read(String key)throws IOException, JSONException {
        JSONObject fullKeyValueStore = new JSONObject(getFileContent());
        if(fullKeyValueStore.has(key)){
            return fullKeyValueStore.getJSONObject(key);
        }
        else{
            return new JSONObject();
        }
    }

    void delete(String key)throws IOException,JSONException{
        JSONObject fullKeyValueStore = new JSONObject(getFileContent());
        fullKeyValueStore.remove(key);
        writeFileContent(fullKeyValueStore.toString());
    }

    int getJSONObjectSizeInKB(JSONObject obj){
        return obj.toString().getBytes().length / 1000;
    }

    void setExpireTimeForKey(String key,int seconds)throws IOException,JSONException{
        new java.util.Timer().schedule(
                new java.util.TimerTask() {
                    @Override
                    public void run(){
                        try{
                            delete(key);
                        }
                        catch (Exception ex){
                            //throw ex;
                            System.out.println("Error occurred in delete schedular " + ex.toString());
                        }
                    }
                },
                seconds*1000
        );
    }

    String getFileContent()throws IOException{
        fileReader = new FileReader(this.filePath);
        int ch;
        String content = "";
        while ((ch = fileReader.read()) != -1){
            content += (char)ch;
        }
        fileReader.close();
        return content;

    }

    void writeFileContent(String content)throws IOException{
        fileWriter = new FileWriter(this.filePath);
        for (int i = 0; i < content.length(); i++)
            fileWriter.write(content.charAt(i));
        fileWriter.close();
    }


}

class DataStore{

    public static void main(String[] args)throws Exception{
        try {
            Scanner scanner = new Scanner(System.in);

            KeyValueStore ks;
            System.out.println("Enter filepath with file name(optional): Enter $ to skip");
            String filepath = scanner.next();
            if(filepath.equals("$")){
                ks = new KeyValueStore();
            }
            else{
                ks = new KeyValueStore(filepath);
            }

            //OPTIONS
            //1 -- CREATE
            //2 -- READ
            //3 -- DELETE
            //4 -- EXIT
            int option;

            do {
                System.out.println("\n\nSelect any option\n\n1 -- CREATE\n2 -- READ\n3 -- DELETE\n");
                option = scanner.nextInt();

                switch (option) {
                    case 1:
                        System.out.println("Enter key:");
                        String key = scanner.next();
                        System.out.println("Enter value(JSON Object):\n");
                        String contin = "y";
                        JSONObject value = new JSONObject();
                        while (contin.equals("y")) {
                            System.out.println("Enter key for JSON Object:");
                            String subKey = scanner.next();
                            System.out.println("Enter value for JSON Object:");
                            String subVal = scanner.next();
                            value.put(subKey, subVal);
                            System.out.println("Do you want to enter another key value for JSON Object(y/n):");
                            contin = scanner.next();
                        }
                        System.out.println("Enter time-to-live period(Seconds): -1 for not set");
                        int ttl = scanner.nextInt();
                        ks.create(key, value, ttl);
                        break;

                    case 2:
                        System.out.println("Enter key to fetch value:");
                        String inKey = scanner.next();
                        JSONObject result = ks.read(inKey);
                        if (result.length() == 0) {
                            System.out.println("Key not present!!!");
                        } else {
                            System.out.println(result.toString());
                        }
                        break;

                    case 3:
                        System.out.println("Enter key to delete:");
                        String delkey = scanner.next();
                        JSONObject res = ks.read(delkey);
                        if (res.length() == 0) {
                            System.out.println("Key not present!!!");
                        } else {
                            ks.delete(delkey);
                            System.out.println("Deleted!!!");
                        }
                        break;
                    default:
                        System.out.println("Exiting...!");
                }

            } while (option >= 1 && option <= 3);

        }
        catch (Exception ex){
            System.out.println("Exception Occured : " + ex.toString());
        }
        /**JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","abc");
        jsonObject.put("sex","M");
        jsonObject.put("Age","21");
        ks.create("temp",jsonObject);**/
    }
}