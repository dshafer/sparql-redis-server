/*
 * (C) Copyright 2011 - Juan F. Sequeda and Daniel P. Miranker
 * Permission to use this code is only granted by separate license 
 */
package translate.sql;

import java.io.PrintStream;

public class PrintUtil 
{

	public static void printSpace(PrintStream out, int count) 
	{
		for (int i = 0; i < count; ++i) 
		{
			out.print(" ");
		}
	}
}