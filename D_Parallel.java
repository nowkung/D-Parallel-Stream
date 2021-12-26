import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple Java program to read CSV file in Java. In this program we will read * list of books stored in CSV file as comma separated values. * * @author WINDOWS 8 *
 */
public class D_Parallel {
    public static void main(String... args) {
        List<Transaction> transactions = readTransaction("5000000 BT Records.csv");
//        // Task 1
        System.out.println("Stream: ");
        long t1 = System.currentTimeMillis();
        List<Transaction> Dupe = transactions.stream().filter(e -> e.getBalance() == 0).collect(Collectors.toList());
        List<Transaction> noDupe = new ArrayList<Transaction>();
        noDupe.add(Dupe.get(0));
        for (int i = 0; i < Dupe.size(); i++) {
            boolean check = false;
            for (int j = 0; j < noDupe.size(); j++) {
                if (Dupe.get(i).getDescription().equals(noDupe.get(j).getDescription())) {
                    check = true;
                    break;
                }
            }
            if (check == false) {
                noDupe.add(Dupe.get(i));
            }
        }
        for (int i = 0; i < noDupe.size(); i++) {
            System.out.println(noDupe.get(i));
        }
        long t2 = System.currentTimeMillis();
        System.out.println("------------------------------------------------------------");
        System.out.println("ParallelStream: ");
        long t3 = System.currentTimeMillis();
        List<Transaction> Dupe1 = transactions.parallelStream().filter(e -> e.getBalance() == 0).collect(Collectors.toList());
        List<Transaction> noDupe1 = new ArrayList<Transaction>();
        noDupe.add(Dupe1.get(0));
        for (int i = 0; i < Dupe1.size(); i++) {
            boolean check = false;
            for (int j = 0; j < noDupe1.size(); j++) {
                if (Dupe1.get(i).getDescription().equals(noDupe1.get(j).getDescription())) {
                    check = true;
                    break;
                }
            }
            if (check == false) {
                noDupe1.add(Dupe1.get(i));
            }
        }
        for (int i = 0; i < noDupe1.size(); i++) {
            System.out.println(noDupe1.get(i));
        }
        long t4 = System.currentTimeMillis();
        System.out.print("StreamTime: ");
        System.out.println(t2-t1);
        System.out.print("ParallelStreamTime: ");
        System.out.println(t4-t3);

        // Task 2

        String[] month = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"};

        // Task 2.1
        // Stream
        t1 = System.currentTimeMillis();
        System.out.println("Stream: ");
        Optional<Transaction> first1 = transactions.stream().findFirst();
        long count = transactions.stream().count();
        String last = transactions.stream().reduce((first, second) -> second).orElse(null).getDate();
        String[] temp = first1.get().getDate().split("-");
        String[] temp2 = last.split("-");
        int firstYear = Integer.parseInt(temp[1]);
        int lastYear = Integer.parseInt(temp2[1]);

        for(int i=firstYear ; i <lastYear; i++){
            for(int j = 0; j < month.length;j++){
                String monthly = month[j]+"-"+i;
                Map<String, Double> deposit = transactions.stream().collect(Collectors.groupingBy(Transaction::getDate,
                                Collectors.summingDouble(Transaction::getDeposit)));
                Map<String, Double> withdraw = transactions.stream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getWithdraw)));
                if(deposit.get(monthly) != null || withdraw.get(monthly) != null) {
                    if (deposit.get(monthly) > 0 || withdraw.get(monthly) > 0) {
                        if (deposit.get(monthly) > 1000000 || withdraw.get(monthly) > 1000000) {
                            double deposits;
                            double withdrawls;
                            deposits = deposit.get(monthly) / 1000000;
                            withdrawls = withdraw.get(monthly) / 1000000;
                            System.out.print(monthly + ": Deposit= ");
                            System.out.format("%.4f", deposits);
                            System.out.print(" M" + ", Withdraw= ");
                            System.out.format("%.4f", withdrawls);
                            System.out.print(" M");
                            System.out.println();
                        }
                    }
                }

            }
        }
        t2 = System.currentTimeMillis();
        // Parallel
        t3 = System.currentTimeMillis();
        System.out.println("ParallelStream: ");
        for(int i=firstYear ; i <lastYear; i++){
            for(int j = 0; j < month.length;j++){
                String monthly = month[j]+"-"+i;
                Map<String, Double> deposit = transactions.parallelStream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getDeposit)));
                Map<String, Double> withdraw = transactions.parallelStream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getWithdraw)));
                if(deposit.get(monthly) != null || withdraw.get(monthly) != null) {
                    if (deposit.get(monthly) > 0 || withdraw.get(monthly) > 0) {
                        if (deposit.get(monthly) > 1000000 || withdraw.get(monthly) > 1000000) {
                            double deposits;
                            double withdrawls;
                            deposits = deposit.get(monthly) / 1000000;
                            withdrawls = withdraw.get(monthly) / 1000000;
                            System.out.print(monthly + ": Deposit= ");
                            System.out.format("%.4f", deposits);
                            System.out.print(" M" + ", Withdraw= ");
                            System.out.format("%.4f", withdrawls);
                            System.out.print(" M");
                            System.out.println();
                        }
                    }
                }

            }
        }
        t4 = System.currentTimeMillis();
        System.out.print("StreamTime: ");
        System.out.println(t2 - t1);
        System.out.print("ParallelStreamTime: ");
        System.out.println(t4 - t3);

        // Task 2.2
        // Stream
        t1 = System.currentTimeMillis();
        System.out.println("Stream: ");
        first1 = transactions.stream().findFirst();
        count = transactions.stream().count();
        last = transactions.stream().reduce((first, second) -> second).orElse(null).getDate();
        temp = first1.get().getDate().split("-");
        temp2 = last.split("-");
        firstYear = Integer.parseInt(temp[1]);
        lastYear = Integer.parseInt(temp2[1]);
        double balance = 0;
        if(first1.get().getDeposit()>0){
            balance = first1.get().getBalance();
        }
        if(first1.get().getWithdraw()>0){
            balance = first1.get().getBalance()*2;
        }
        for(int i=firstYear ; i <lastYear; i++){
            for(int j = 0; j < month.length;j++){
                String monthly = month[j]+"-"+i;
                Map<String, Double> deposit = transactions.stream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getDeposit)));
                Map<String, Double> withdraw = transactions.stream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getWithdraw)));
                if(deposit.get(monthly) != null || withdraw.get(monthly) != null) {
                    balance = balance+(deposit.get(monthly)-withdraw.get(monthly));
                    if (deposit.get(monthly) > 0 || withdraw.get(monthly) > 0) {
                        double tempo = balance;
                        if(balance>1000000) {
                            tempo = tempo/1000000;
                            System.out.print(monthly + ": Balance= ");
                            System.out.format("%.4f",tempo);
                            System.out.print(" M");
                            System.out.println();
                        }else {
                            System.out.print(monthly + ": Balance= ");
                            System.out.format("%.4f",tempo);
                            System.out.println();
                        }
                    }
                }

            }
        }
        t2 = System.currentTimeMillis();
        // Parallel
        t3 = System.currentTimeMillis();
        System.out.println("ParallelStream: ");
        first1 = transactions.stream().parallel().findFirst();
        balance = 0;
        if(first1.get().getDeposit()>0){
            balance = first1.get().getBalance();
            System.out.println(balance);
        }
        if(first1.get().getWithdraw()>0){
            balance = first1.get().getBalance()*2;
            System.out.println(balance);
        }
        for(int i=firstYear ; i <lastYear; i++){
            for(int j = 0; j < month.length;j++){
                String monthly = month[j]+"-"+i;
                Map<String, Double> deposit = transactions.parallelStream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getDeposit)));
                Map<String, Double> withdraw = transactions.parallelStream().collect(Collectors.groupingBy(Transaction::getDate,
                        Collectors.summingDouble(Transaction::getWithdraw)));
                if(deposit.get(monthly) != null || withdraw.get(monthly) != null) {
                    balance = balance+(deposit.get(monthly)-withdraw.get(monthly));
                    if (deposit.get(monthly) > 0 || withdraw.get(monthly) > 0) {
                        double tempo = balance;
                        if(balance>1000000) {
                            tempo = tempo/1000000;
                            System.out.print(monthly + ": Balance= ");
                            System.out.format("%.4f",tempo);
                            System.out.print(" M");
                            System.out.println();
                        }else {
                            System.out.print(monthly + ": Balance= ");
                            System.out.format("%.4f",tempo);
                            System.out.println();
                        }
                    }
                }

            }
        }
        t4 = System.currentTimeMillis();
        System.out.print("StreamTime: ");
        System.out.println(t2 - t1);
        System.out.print("ParallelStreamTime: ");
        System.out.println(t4 - t3);


}

    private static List<Transaction> readTransaction(String fileName) {
//        PrintWriter writer;
        List<Transaction> transactions = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);
        try (BufferedReader br = Files.newBufferedReader(pathToFile, StandardCharsets.US_ASCII)) {
            String line = br.readLine();
            line = br.readLine();
//            writer = new PrintWriter("C:\\Users\\USER\\Desktop\\D-Parallel Computing\\record.txt", "UTF-8");

            while (line != null) {
                Object[] attributes = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                Transaction transaction = createTransaction(attributes);
                transactions.add(transaction);
//                writer.println(transaction.getDate()+"|"+transaction.getDescription()+"|"+transaction.getDeposit()+"|"+transaction.getWithdraw()+"|"+transaction.getBalance());
                line = br.readLine();
            }
//            writer.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return transactions;
    }

    private static Transaction createTransaction(Object[] metadata) {
        String date = metadata[0].toString();
        String description = metadata[1].toString();
        double deposit = Double.parseDouble(metadata[2].toString().replace("\"", "").replace(",", ""));
        double withdraw = Double.parseDouble(metadata[3].toString().replace("\"", "").replace(",", ""));
        double balance = Double.parseDouble(metadata[4].toString().replace("\"", "").replace(",", ""));
        return new Transaction(date, description, deposit, withdraw, balance);
    }
}

class Transaction {
    private String date;
    private String description;
    private double deposit;
    private double withdraw;
    private double balance;

    public Transaction(String date, String description, double deposit, double withdraw, double balance) {
        this.date = date;
        this.description = description;
        this.deposit = deposit;
        this.withdraw = withdraw;
        this.balance = balance;
    }

    public String getDate() {
        return date.substring((date.length()-8),date.length());
    }

    public String getDescription() {
        return description;
    }

    public double getDeposit() {
        return deposit;
    }

    public double getWithdraw() {
        return withdraw;
    }

    public double getBalance() {
        return balance;
    }

    @Override
    public String toString() {
        return "Transaction [date=" + date + ", description=" + description + ", deposit=" + deposit + ", withdraw=" + withdraw + ", balance=" + balance + "]";
    }
}

