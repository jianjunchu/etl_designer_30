package org.pentaho.di.core.util;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class LicenseManager {

    private static final String FILE_NAME = "license.txt";
    private static final int KEY = 3; // 加密和解密的密钥
    private static final int RANDOM_STRING_LENGTH = 50; // 随机字符串的长度
    private static final String PREFIX = "START_"; // 前缀
    private static final String SUFFIX = "_END"; // 后缀
    private static final long VALID_DAYS = 36500; // license 有效天数

    // 生成 license 文件
    public static void generateLicense() {
        String randomString = generateRandomString(RANDOM_STRING_LENGTH);
        String hash = calculateHash(randomString);
        long timestamp = System.currentTimeMillis();
        String fullLicense = PREFIX + randomString + "|" + hash + "|" + timestamp + SUFFIX;
        String encryptedString = encrypt(fullLicense);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_NAME))) {
            writer.write(encryptedString);
            System.out.println("License file generated successfully.");
        } catch (IOException e) {
            System.err.println("Error generating license file: " + e.getMessage());
        }
    }

    // 读取 license 文件并验证
    public static boolean verifyLicense() {
        try (BufferedReader reader = new BufferedReader(new FileReader(FILE_NAME))) {
            String encryptedString = reader.readLine();
            String decryptedString = decrypt(encryptedString);
            return isValidLicense(decryptedString);
        } catch (IOException e) {
            System.err.println("Error reading license file: " + e.getMessage());
            return false;
        }
    }

    // 生成随机字符串
    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    // 加密方法
    private static String encrypt(String input) {
        StringBuilder encrypted = new StringBuilder();
        for (char c : input.toCharArray()) {
            encrypted.append((char) (c + KEY));
        }
        return encrypted.toString();
    }

    // 解密方法
    private static String decrypt(String input) {
        StringBuilder decrypted = new StringBuilder();
        for (char c : input.toCharArray()) {
            decrypted.append((char) (c - KEY));
        }
        return decrypted.toString();
    }

    // 计算哈希值
    private static String calculateHash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    // 验证 license 是否有效
    private static boolean isValidLicense(String license) {
        if (!license.startsWith(PREFIX) || !license.endsWith(SUFFIX)) {
            return false;
        }
        String content = license.substring(PREFIX.length(), license.length() - SUFFIX.length());
        String[] parts = content.split("\\|");
        if (parts.length != 3) {
            return false;
        }
        String randomString = parts[0];
        String storedHash = parts[1];
        long timestamp = Long.parseLong(parts[2]);

        // 验证哈希值
        String calculatedHash = calculateHash(randomString);
        if (!storedHash.equals(calculatedHash)) {
            return false;
        }

        // 验证时间戳
        long currentTime = System.currentTimeMillis();
        long validDuration = VALID_DAYS * 24 * 60 * 60 * 1000;
        if (currentTime - timestamp > validDuration) {
            return false;
        }

        return true;
    }

    public static void main(String[] args) {
        // 生成 license 文件
        generateLicense();
        // 验证 license 文件
        //boolean isValid = verifyLicense();
        //System.out.println("License is valid: " + isValid);
    }
}
