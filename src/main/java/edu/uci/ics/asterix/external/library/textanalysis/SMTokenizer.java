package edu.uci.ics.asterix.external.library.textanalysis;


public class SMTokenizer extends AbstractTokenizer implements ITokenizer {
    private enum States {
        READY,
        IN_NUMBER,
        IN_VARIABLE,
        ERROR
    };

    private String input;
    private int inputLength;
    private int position;
    
    public static SMTokenizer INSTANCE = new SMTokenizer();

    private SMTokenizer() {
    }
    
    public void reset(String input) {
        this.input = input.trim() + " ";
        this.inputLength = input.length();
        position = -1;
    }

    public boolean hasNext() {
        return position < inputLength - 2;
    }

    public Token next() {
        States state;
        String value = "";

        if (!hasNext()) {
            throw new IllegalStateException("No more tokens!");
        }

        state = States.READY;
        while ((++position) < input.length()) {
            char ch = input.charAt(position);
            switch (state) {
                case READY:
                    value = ch + "";
                    if (Character.isWhitespace(ch)) {
                        break;
                    }
                    if ("()".contains(ch + "")) {
                        return new Token(TokenType.PARENTHESIS, value);
                    }
                    if ("+-*/%".contains(ch + "")) {
                        return new Token(TokenType.OPERATOR, value);
                    }
                    if (Character.isLetter(ch)) {
                        state = States.IN_VARIABLE;
                        break;
                    }
                    if (Character.isDigit(ch)) {
                        state = States.IN_NUMBER;
                        break;
                    }
                    return new Token(TokenType.ERROR, value);
                case IN_VARIABLE:
                    if (Character.isLetter(ch) || Character.isDigit(ch)) {
                        value += ch;
                        break;
                    } else {
                        position--; // save char for next time
                        return new Token(TokenType.VARIABLE, value);
                    }

                case IN_NUMBER:
                    if (Character.isDigit(ch)) {
                        value += ch;
                        break;
                    } else {
                        position--; // save char for next time
                        return new Token(TokenType.INTEGER, value);
                    }
                default:
                    return new Token(TokenType.ERROR, value);

            }
        }
        assert false; // should never get here
        return null;
    }

    @Override
    public String[] tokenize(String text) {
        reset(text);
        String temp[] = new String[text.length()/2 + 2]; 
        int count=0;
        while (hasNext()) {
            Token token = next();
            if (token.getType().equals(TokenType.VARIABLE)) {
                temp[count] = token.getValue();
                count++;
            }
        }
        
        String result[] = new String[count];
        System.arraycopy(temp, 0, result, 0, count);
        temp = null;
        return result;
    }

    @Override
    public String[] tokenize(String text, boolean removeStopWord) {
        // TODO Auto-generated method stub
        return null;
    }

}
