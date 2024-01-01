package com.github.springtestdbunit;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.core.annotation.AnnotationUtils;

class Annotations<T extends Annotation> implements Iterable<T> {

    private final List<T> classAnnotations;

    private final List<T> methodAnnotations;

    private final List<T> allAnnotations;

    Annotations(Class<?> testClass, Method testMethod, Class<T> annotation) {
	this.classAnnotations = getAnnotations(testClass, annotation);
	this.methodAnnotations = getAnnotations(testMethod, annotation);
	List<T> allAnnotations = new ArrayList<>(this.classAnnotations.size() + this.methodAnnotations.size());
	allAnnotations.addAll(this.classAnnotations);
	allAnnotations.addAll(this.methodAnnotations);
	this.allAnnotations = Collections.unmodifiableList(allAnnotations);
    }

    private List<T> getAnnotations(AnnotatedElement element, Class<T> annotation) {
	Set<T> repeatableAnnotation = AnnotationUtils.getRepeatableAnnotations(element, annotation);
	return Collections.unmodifiableList(new ArrayList<>(repeatableAnnotation));
    }

    public List<T> getClassAnnotations() {
	return this.classAnnotations;
    }

    public List<T> getMethodAnnotations() {
	return this.methodAnnotations;
    }

    @Override
    public Iterator<T> iterator() {
	return this.allAnnotations.iterator();
    }
}
