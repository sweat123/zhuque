package com.laomei.zhuque;

import com.laomei.zhuque.util.PlaceholderParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * @author luobo
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class PlaceholderParserTest {

    private PlaceholderParser placeholderParser;

    @Before
    public void init() {
        Map<String, Object> context = new HashMap<>();
        context.put("fieldA", "valueA");
        Map<String, Object> mapValue = new HashMap<>();
        mapValue.put("fieldD", "valueD");
        Map<String, Object> subMapValue = new HashMap<>(1);
        subMapValue.put("fieldF", "valueF");
        mapValue.put("fieldE", subMapValue);
        context.put("fieldC", mapValue);
        placeholderParser = PlaceholderParser.getParser(context);
    }

    @Test
    public void testParser() {
        String placeholder1 = "${fieldA}";
        String placeholder2 = "${fieldC.fieldD}";
        String placeholder3 = "${fieldC.fieldE.fieldF}";
        Assert.assertEquals("valueA", placeholderParser.replacePlaceholder(placeholder1));
        Assert.assertEquals("valueD", placeholderParser.replacePlaceholder(placeholder2));
        Assert.assertEquals("valueF", placeholderParser.replacePlaceholder(placeholder3));
    }
}
