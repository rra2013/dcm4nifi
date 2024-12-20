package org.rra.cmove;

@FunctionalInterface
public interface MoveHasErrors {
    void moveHasError(int staus, String message) throws Exception;
}
