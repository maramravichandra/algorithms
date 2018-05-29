package com.ds;

public class CustomStack<E> {

	private int size = 10;
	private int top = -1;
	private Object[] stack;
	
	public CustomStack(){
		size = 10;
		top = -1;
		stack = new Object[size];
	}
	
	public CustomStack(int size){
		this.size = size;
		stack = new Object[size];
	}
	
	public boolean push( E element ){
		if( isFull() )
			return false;
		else{
			top++;
			stack[top] = element;
			System.out.println("top "+top);
			return true;
		}
	}
	
	@SuppressWarnings("unchecked")
	public E pop(){
		E ele = (E) stack[top--];
		if( top != -1 ){
			stack[top] = null;
		}
		return ele;
	}
	
	public boolean isFull(){
		return top == size - 1;
	}
}
