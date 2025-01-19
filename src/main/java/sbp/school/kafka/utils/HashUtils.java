package sbp.school.kafka.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * Класс для работы с хеш-ами
 */
public class HashUtils {
    private static final String MD5 = "MD5";

    /**
     * Подсчитать хеш-сумму строк с помощью алгоритма MD5
     *
     * @param strings строки
     * @return хэш-сумма в шестнадцатеричном представлении
     * @throws Exception исключение при ошибке хэширования
     */
    public static String calcHashSum(List<String> strings) throws Exception {
        StringBuilder strBuilder = new StringBuilder();

        strings.stream()
                .sorted()
                .forEach(strBuilder::append);

        MessageDigest md = MessageDigest.getInstance(MD5);

        md.update(strBuilder.toString().getBytes(StandardCharsets.UTF_8));

        return encodeHexString(md.digest());
    }

    /**
     * Преобразовать массив байт в шестнадцатеричное представление
     *
     * @param byteArray массив байт
     * @return шестнадцатеричное представление
     */
    private static String encodeHexString(byte[] byteArray) {
        StringBuilder hexStringBuffer = new StringBuilder();

        for (byte b : byteArray) {
            hexStringBuffer.append(byteToHex(b));
        }
        return hexStringBuffer.toString();
    }

    /**
     * Преобразовать байт в шестнадцатеричное представление
     *
     * @param num байти
     * @return шестнадцатеричное представление
     */
    private static String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);

        return new String(hexDigits);
    }
}
