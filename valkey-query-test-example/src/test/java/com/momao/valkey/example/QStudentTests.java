package com.momao.valkey.example;

import com.momao.valkey.core.SearchCondition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StudentQueryTests {

    private static final StudentQuery qStudent = new StudentQuery();

    @Test
    void textFieldEq() {
        SearchCondition condition = qStudent.name.eq("Alice");
        assertEquals("@name:Alice", condition.build());
    }

    @Test
    void textFieldWithSpecialChars() {
        SearchCondition condition = qStudent.name.eq("john-doe@gmail.com");
        assertEquals("@name:john\\-doe\\@gmail\\.com", condition.build());
    }

    @Test
    void textFieldIn() {
        SearchCondition condition = qStudent.name.in("Alice", "Bob", "Charlie");
        assertEquals("@name:(Alice | Bob | Charlie)", condition.build());
    }

    @Test
    void textFieldStartsWith() {
        SearchCondition condition = qStudent.name.startsWith("Al");
        assertEquals("@name:Al*", condition.build());
    }

    @Test
    void textFieldContains() {
        SearchCondition condition = qStudent.name.contains("ice");
        assertEquals("@name:ice", condition.build());
    }

    @Test
    void numericFieldEq() {
        SearchCondition condition = qStudent.age.eq(18);
        assertEquals("@age:[18 18]", condition.build());
    }

    @Test
    void numericFieldGte() {
        SearchCondition condition = qStudent.age.gte(18);
        assertEquals("@age:[18 +inf]", condition.build());
    }

    @Test
    void numericFieldLte() {
        SearchCondition condition = qStudent.age.lte(25);
        assertEquals("@age:[-inf 25]", condition.build());
    }

    @Test
    void numericFieldGt() {
        SearchCondition condition = qStudent.age.gt(18);
        assertEquals("@age:[(18 +inf]", condition.build());
    }

    @Test
    void numericFieldLt() {
        SearchCondition condition = qStudent.age.lt(25);
        assertEquals("@age:[-inf (25)]", condition.build());
    }

    @Test
    void numericFieldBetween() {
        SearchCondition condition = qStudent.age.between(18, 25);
        assertEquals("@age:[18 25]", condition.build());
    }

    @Test
    void numericFieldIn() {
        SearchCondition condition = qStudent.age.in(18, 19, 20);
        assertEquals("@age:([18 18] | [19 19] | [20 20])", condition.build());
    }

    @Test
    void customFieldName() {
        SearchCondition condition = qStudent.className.eq("Class A");
        assertEquals("@class_name:Class\\ A", condition.build());
    }

    @Test
    void combineConditions() {
        SearchCondition condition = qStudent.name.eq("Alice")
                .and(qStudent.age.between(18, 25));
        assertEquals("(@name:Alice @age:[18 25])", condition.build());
    }

    @Test
    void complexCondition() {
        SearchCondition condition = qStudent.name.startsWith("Al")
                .or(qStudent.age.gte(18)
                        .and(qStudent.score.gte(90.0)));
        assertEquals("(@name:Al* | (@age:[18 +inf] @score:[90.0 +inf]))", condition.build());
    }

    @Test
    void notCondition() {
        SearchCondition condition = qStudent.name.eq("Admin").not();
        assertEquals("-(@name:Admin)", condition.build());
    }

    @Test
    void tagFieldEq() {
        SearchCondition condition = qStudent.department.eq("Backend");
        assertEquals("@department:{Backend}", condition.build());
    }

    @Test
    void tagFieldWithSpecialChars() {
        SearchCondition condition = qStudent.department.eq("Back-end:Dev");
        assertEquals("@department:{Back\\-end\\:Dev}", condition.build());
    }

    @Test
    void tagFieldIn() {
        SearchCondition condition = qStudent.department.in("Backend", "AI", "Frontend");
        assertEquals("@department:{Backend | AI | Frontend}", condition.build());
    }

    @Test
    void tagFieldNot() {
        SearchCondition condition = qStudent.department.eq("Admin").not();
        assertEquals("-(@department:{Admin})", condition.build());
    }

    @Test
    void tagAndTextCombined() {
        SearchCondition condition = qStudent.department.in("Backend", "AI")
                .and(qStudent.name.eq("Admin").not());
        assertEquals("(@department:{Backend | AI} -(@name:Admin))", condition.build());
    }

    @Test
    void allTypesCombined() {
        SearchCondition condition = qStudent.department.eq("Backend")
                .and(qStudent.age.between(18, 30))
                .and(qStudent.name.startsWith("A"))
                .and(qStudent.status.eq("ACTIVE").not());
        assertEquals("(((@department:{Backend} @age:[18 30]) @name:A*) -(@status:{ACTIVE}))", condition.build());
    }
}
