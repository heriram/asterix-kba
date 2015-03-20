package edu.uci.ics.asterix.external.library.udf.featuregeneration;

/**
 * Taking care of the postings for the posting list
 * @author heri
 *
 */
public class Posting {
    int phraseIndex;
    int termPosition;
  
    
    public Posting(int phraseIndex, int termPosition) {
        this.phraseIndex = phraseIndex;
        this.termPosition = termPosition;
    }
    
    @Override
    public String toString() {
        return "<" + phraseIndex + "," + termPosition + ">";
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + phraseIndex;
        result = prime * result + termPosition;
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Posting other = (Posting) obj;
        return (other.phraseIndex==this.phraseIndex && 
                other.termPosition==this.termPosition);
    }

}
