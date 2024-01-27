package com.example.dtabaseminiproject.dal;

public class RLEConversion {
    public String encodeToRLE(String input) {
        StringBuilder encoded = new StringBuilder();
        int count = 0;

        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) == '0') {
                count++;
            } else {
                if(count==0) {
                    encoded.append("2");
                } else {
                    encoded.append(unaryEncode(count));
                    encoded.append(Integer.toBinaryString(count));
                    count = 0;
                }
            }
        }
        if (count > 0) {
            encoded.append("3".repeat(count));
        }
        return encoded.toString();
    }

    public String decodeFromRLE(String encoded) {
        StringBuilder decoded = new StringBuilder();

        int n = encoded.length() - 1;
        while(encoded.charAt(n)=='3')
        {
            n--;
        }
        String temp = "0".repeat(encoded.length() - 1 - n);

        int i = 0;
        int count = 0;
        while (i <= n) {
            if(encoded.charAt(i)=='1') {
                count++;
                i++;
            }
            else if(encoded.charAt(i)=='0') {
                i++;
                count++;
                int c = Integer.parseInt(encoded.substring(i,i+count), 2);
                decoded.append("0".repeat(c)).append("1");
                i = i + count;
                count=0;
            }
            else {
                decoded.append("1");
                count = 0;
                i++;
            }
        }
        decoded.append(temp);
        return decoded.toString();
    }

    private String unaryEncode(int n) {
        int c = Integer.toBinaryString(n).length();
        return "1".repeat(c-1) + "0";
    }
}
