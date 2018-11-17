package com.cris;

/**
 * ETL 工具类
 *
 * @author zc-cris
 * @version 1.0
 **/
public class EtlStringUtil {

    private static StringBuilder stringBuilder = new StringBuilder();
    private static final int STANDARD_LENGTH = 9;
    private static final int STANDARD_LENGTH_SUB_1 = 8;


    /**
     * 使用 main 方法做测试，保证工具类的可用性
     *
     * @param args main 方法的参数
     */
    public static void main(String[] args) {
        String handle = EtlStringUtil.handle("SDNkMu8ZT68\tw00dy911\t630\tPeople & Blogs\t186\t10181\t3.49\t494\tcris\tloveu\tsimida".split("\t"));
        System.out.println("handle = " + handle);
    }

    /**
     * 将输入的字符串数组转换为合格的数据
     *
     * @param strings 待处理的字符串数组
     * @return  处理后的字符串
     */
    static String handle(String[] strings) {

        // 一定要清除原 StringBuilder 对象的字符串内容！！！
        stringBuilder.delete(0, stringBuilder.length());

        if (strings.length < STANDARD_LENGTH) {
            return null;
        }
        strings[3] = strings[3].replaceAll(" ", "");

        if (strings.length == STANDARD_LENGTH) {
            for (int i = 0; i < STANDARD_LENGTH_SUB_1; i++) {
                stringBuilder.append(strings[i]).append("\t");
            }
            stringBuilder.append(strings[8]);
        } else {
            for (int i = 0; i < STANDARD_LENGTH; i++) {
                stringBuilder.append(strings[i]).append("\t");
            }
            for (int i = 9; i < strings.length - 1; i++) {
                stringBuilder.append(strings[i]).append("&");
            }
            stringBuilder.append(strings[strings.length - 1]);
        }
        return stringBuilder.toString();
    }
}
