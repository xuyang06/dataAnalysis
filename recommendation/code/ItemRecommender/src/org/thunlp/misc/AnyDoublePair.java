package org.thunlp.misc;

public class AnyDoublePair<FirstType> {
	public FirstType first;
	public double second;
	
	public AnyDoublePair() {
		first = null;
		second = 0;
	}
	public AnyDoublePair(FirstType first, double second) {
		this.first = first;
		this.second = second;
	}
	
	public String toString() {
		return first + " " + second;
	}
}
