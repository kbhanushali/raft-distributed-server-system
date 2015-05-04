package poke.server.managers.src;

/*************************************************************************
 *  Compilation:  javac Grayscale.java
 *  Execution:    java Grayscale filename
 *
 *  Reads in an image from a file, converts it to grayscale, and
 *  displays it on the screen.
 *
 *  % java Grayscale image.jpg
 *
 *************************************************************************/

import java.awt.Color;

public class Grayscale {

	//public static String inp_path = target_dir+req.getPayload().getImgId()+"/toprocess/"";
	//public static String out_path = "../images/out/";
	
    public static String applyMask(String name)
    {
    	String inp_img=name;
    	String out_img=name.replace("toprocess", "processed");
    	
    	Picture pic = new Picture(inp_img);
        int width  = pic.width();
        int height = pic.height();

        // convert to grayscale
        for (int col = 0; col < width; col++) {
            for (int row = 0; row < height; row++) {
                Color color = pic.get(col, row);
                Color gray = Luminance.toGray(color);
                pic.set(col, row, gray);
            }
        }
       
        pic.save(out_img);
        return out_img;

    }

}