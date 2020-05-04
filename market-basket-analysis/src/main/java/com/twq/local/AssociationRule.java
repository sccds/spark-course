package com.twq.local;

import java.util.List;

/**
 * 关联规则实体类
 */
public class AssociationRule {

    private List<String> antecedent;  // 前件项集
    private List<String> consequent;  // 后件项集
    private double confidence;         // 置信度
    private double support;     // 支持度

    public AssociationRule(List<String> antecedent, List<String> consequent, double confidence, double support) {
        this.antecedent = antecedent;
        this.consequent = consequent;
        this.confidence = confidence;
        this.support = support;
    }

    public List<String> getAntecedent() {
        return antecedent;
    }

    public void setAntecedent(List<String> antecedent) {
        this.antecedent = antecedent;
    }

    public List<String> getConsequent() {
        return consequent;
    }

    public void setConsequent(List<String> consequent) {
        this.consequent = consequent;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public double getSupport() {
        return support;
    }

    public void setSupport(double support) {
        this.support = support;
    }
}
