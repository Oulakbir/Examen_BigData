package ma.oulakbir.Exercice1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Exercice1Application {

    public static void main(String[] args) {
        SpringApplication.run(Exercice1Application.class, args);
    }

}
