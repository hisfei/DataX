package com.alibaba.datax.plugin.writer.doriswriter;

import com.alibaba.datax.common.element.Record;

public class DorisCsvCodec extends DorisBaseCodec implements DorisCodec {

    private static final long serialVersionUID = 1L;

    private final String columnSeparator;

    public DorisCsvCodec ( String sp) {
        this.columnSeparator = DelimiterParser.parse(sp, "\t");
    }

    @Override
     public String codec( Record row) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < row.getColumnNumber(); i++) {
            String value = convertionField(row.getColumn(i));
            if (value == null) {
                sb.append("\\N");
            } else {
                // 只转义 会破坏格式的字符，不包引号
                sb.append(escapeValue(value));
            }

            if (i < row.getColumnNumber() - 1) {
                sb.append(columnSeparator);
            }
        }
        return sb.toString();
    }
    // ==========================================
    // 转义函数：处理 \n \r \t \  等特殊字符
    // 不包引号！完全满足你的要求
    // ==========================================
    private String escapeValue(String s) {
        return s.replace("\\", "\\\\")    // 反斜杠必须转义
                .replace("\n", "\\\\n")     // 换行
                .replace("\r", "\\\\r")     // 回车
                .replace("\t", "\\\\t");    // 制表符
    }
}
