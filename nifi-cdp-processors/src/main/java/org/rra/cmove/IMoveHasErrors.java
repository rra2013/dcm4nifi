package org.rra.cmove;

@FunctionalInterface
public interface IMoveHasErrors {
    void moveHasError(int staus, String message) throws Exception;
}
