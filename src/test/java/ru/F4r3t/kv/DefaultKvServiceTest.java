package ru.F4r3t.kv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.F4r3t.kv.model.KvEntry;
import ru.F4r3t.kv.model.RangePage;
import ru.F4r3t.kv.repository.KvRepository;
import ru.F4r3t.kv.service.DefaultKvService;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultKvServiceTest {

    @Mock
    private KvRepository repository;

    private DefaultKvService service;

    @BeforeEach
    void setUp() {
        service = new DefaultKvService(repository);
    }

    @Test
    void putShouldDelegateToRepository() {
        byte[] value = new byte[] {1, 2, 3};
        when(repository.put("key-1", value)).thenReturn(true);

        boolean result = service.put("key-1", value);

        assertTrue(result);
        verify(repository).put("key-1", value);
    }

    @Test
    void putShouldAllowNullValue() {
        when(repository.put("key-null", null)).thenReturn(false);

        boolean result = service.put("key-null", null);

        assertFalse(result);
        verify(repository).put("key-null", null);
    }

    @Test
    void getShouldReturnRepositoryResult() {
        KvEntry entry = new KvEntry("k", new byte[] {9, 8});
        when(repository.get("k")).thenReturn(Optional.of(entry));

        Optional<KvEntry> result = service.get("k");

        assertTrue(result.isPresent());
        assertEquals("k", result.get().key());
        assertArrayEquals(new byte[] {9, 8}, result.get().value());
        verify(repository).get("k");
    }

    @Test
    void getShouldReturnEmptyWhenRepositoryReturnsEmpty() {
        when(repository.get("missing")).thenReturn(Optional.empty());

        Optional<KvEntry> result = service.get("missing");

        assertTrue(result.isEmpty());
        verify(repository).get("missing");
    }

    @Test
    void deleteShouldDelegateToRepository() {
        when(repository.delete("key-del")).thenReturn(true);

        boolean result = service.delete("key-del");

        assertTrue(result);
        verify(repository).delete("key-del");
    }

    @Test
    void countShouldDelegateToRepository() {
        when(repository.count()).thenReturn(42L);

        long result = service.count();

        assertEquals(42L, result);
        verify(repository).count();
    }

    @Test
    void rangePageShouldDelegateToRepositoryForValidArguments() {
        RangePage page = new RangePage(List.of(new KvEntry("a", new byte[] {1})), null);
        when(repository.rangePage("a", "z", null, 50)).thenReturn(page);

        RangePage result = service.rangePage("a", "z", null, 50);

        assertSame(page, result);
        verify(repository).rangePage("a", "z", null, 50);
    }

    @Test
    void putShouldThrowWhenKeyIsNull() {
        assertThrows(IllegalArgumentException.class, () -> service.put(null, new byte[] {1}));
    }

    @Test
    void getShouldThrowWhenKeyIsNull() {
        assertThrows(IllegalArgumentException.class, () -> service.get(null));
    }

    @Test
    void deleteShouldThrowWhenKeyIsNull() {
        assertThrows(IllegalArgumentException.class, () -> service.delete(null));
    }

    @Test
    void rangePageShouldThrowWhenKeySinceIsNull() {
        assertThrows(IllegalArgumentException.class, () -> service.rangePage(null, "z", null, 10));
    }

    @Test
    void rangePageShouldThrowWhenKeyToIsNull() {
        assertThrows(IllegalArgumentException.class, () -> service.rangePage("a", null, null, 10));
    }

    @Test
    void rangePageShouldThrowWhenKeySinceIsGreaterThanKeyTo() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.rangePage("z", "a", null, 10)
        );

        assertEquals("keySince must be <= keyTo", exception.getMessage());
    }

    @Test
    void rangePageShouldThrowWhenLimitIsNotPositive() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.rangePage("a", "z", null, 0)
        );

        assertEquals("limit must be > 0", exception.getMessage());
    }

    @Test
    void putShouldThrowWhenKeyIsBlank() {
        assertThrows(IllegalArgumentException.class, () -> service.put("   ", new byte[] {1}));
    }

    @Test
    void getShouldThrowWhenKeyIsBlank() {
        assertThrows(IllegalArgumentException.class, () -> service.get(""));
    }

    @Test
    void deleteShouldThrowWhenKeyIsBlank() {
        assertThrows(IllegalArgumentException.class, () -> service.delete(" "));
    }
}
