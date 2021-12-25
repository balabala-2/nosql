package python;

import ml.bundle.Value;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class py {
    public static void main(String[] args) {
        Process proc;

        try {
            proc = Runtime.getRuntime().exec("E://Recommend_system//venv3.7//Scripts//python.exe E:\\Recommend_system\\123\\model.py 1 2 1 3 4 5");// 执行py文件
            InputStream inputStream = proc.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            StringBuilder result = new StringBuilder();
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
            String substring = result.substring(1, result.length() - 1);
            String[] split = substring.split(", ");
            System.out.println(split[0]);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
