package utils;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class AESUtil {
    public static final String InitVector = "EncDecInitVector";
    public static byte[] encrypt(byte[] K_e,byte[] plaintext){
        try{
            IvParameterSpec iv = new IvParameterSpec(InitVector.getBytes(StandardCharsets.UTF_8));
            SecretKeySpec secretKeySpec = new SecretKeySpec(K_e, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec,iv);
            return cipher.doFinal(plaintext);
        } catch (InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        } catch (NoSuchPaddingException e) {
            throw new RuntimeException(e);
        } catch (IllegalBlockSizeException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (BadPaddingException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] decrypt(byte[] K_e,byte[] ciphertext) {
        IvParameterSpec iv = new IvParameterSpec(InitVector.getBytes(StandardCharsets.UTF_8));
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(K_e, "AES");
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec,iv);
            byte[] res = cipher.doFinal(ciphertext);
            return res;
        } catch (Exception e){

        }
        return null;
    }
}