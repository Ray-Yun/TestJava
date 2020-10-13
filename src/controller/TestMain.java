package controller;
//
import java.util.Scanner;

public class TestMain {
	 public static void main(String[] args) {
	        // Animal為Dog及Bird的通用型態
	        Animal dog = new Dog();   // 子類別Dog物件分派至Animal型別變數dog
	        Animal bird = new Bird(); // 子類別Bird物件分派至Animal型別變數bird
	        act(dog);  // run
	        act(bird); // fly
	    }

	    private static void act(Animal animal) { // 因為多型，所以參數以通用的父類別傳入
	        animal.move(); // 因為多型及覆寫，所以實際執行的方法為子類別的方法內容
	    }
	}

	class Animal {
	    public void move() {
	        System.out.println("move");
	    }
	}

	class Dog extends Animal {
	    @Override
	    public void move() {
	        System.out.println("run"); // 覆寫父類別Animal.move()的內容
	    }
	}

	class Bird extends Animal {
	    @Override
	    public void move() {
	        System.out.println("fly"); // 覆寫父類別Animal.move()的內容
	    }
	}
