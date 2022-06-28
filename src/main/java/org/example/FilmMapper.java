package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class FilmMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String data = value.toString();
        // index data dipisahkan dengan koma (,) sebagai pemisah
        // Format   : title,director,stars,IMDb-Rating,Category,Duration,Censor-board-rating,ReleaseYear
        // index 0  : title
        // index 1  : director
        // index 2  : stars
        // index 3  : IMDb-Rating
        // index 4  : Category
        // index 5  : Duration
        // index 6  : Censor-board-rating
        // index 7  : ReleaseYear

        String [] k = data.split("[,]");
        if(k.length == 5) {
            // Disini menggunakan index 5 (durasi) karena akan menghitung jumlah film yang mempunyai durasi lebih dari 1.5 jam
            if(!(k[5].equals(""))) {
                int i = Integer.parseInt(k[5]);
                //1.5 jam = 5400 detik
                if(i > 5400) {
                    Text oa = new Text("Jumlah film yang mempunyai durasi lebih dari 1.5 jam : ");
                    IntWritable ob = new IntWritable(1);
                    context.write(oa, ob);
                }
            }
        }
    }

}
